"""
Various I/O adaptors
"""

from typing import Any, Dict, List, Optional, Union, cast
import json
from urllib.parse import urlparse
import logging
import dask
from dask.delayed import Delayed
from pathlib import Path
import xarray as xr
import io
from rasterio.crs import CRS

from datacube.utils.aws import get_creds_with_retry, mk_boto_session, s3_client
from odc.aws import s3_head_object  # TODO: move it to datacube
from datacube.utils.dask import save_blob_to_s3, save_blob_to_file
from datacube.utils.cog import to_cog
from datacube.model import Dataset
from botocore.credentials import ReadOnlyCredentials
from .model import Task, EXT_TIFF
from .plugins import StatsPluginInterface
from hashlib import sha1
from collections import namedtuple
from odc.stats import __version__

from eodatasets3.assemble import serialise
from eodatasets3.scripts.tostac import json_fallback
from eodatasets3.model import DatasetDoc
from eodatasets3.images import FileWrite, GridSpec
import eodatasets3.stac as eo3stac
import eodatasets3

WriteResult = namedtuple("WriteResult", ["path", "sha1", "error"])

_log = logging.getLogger(__name__)
DEFAULT_COG_OPTS = dict(compress="deflate", zlevel=6, blocksize=512,)


def load_creds(profile: Optional[str] = None) -> ReadOnlyCredentials:
    session = mk_boto_session(profile=profile)
    creds = get_creds_with_retry(session)
    if creds is None:
        raise ValueError("Failed to obtain credentials")

    return creds.get_frozen_credentials()


def dump_json(meta: Dict[str, Any]) -> str:
    return json.dumps(meta, separators=(",", ":"))


def mk_sha1(data):
    if isinstance(data, str):
        data = data.encode("utf8")
    return sha1(data).hexdigest()


_dask_sha1 = dask.delayed(mk_sha1, name="sha1")


@dask.delayed
def _pack_write_result(write_out, sha1):
    path, ok = write_out
    if ok:
        return WriteResult(path, sha1, None)
    else:
        return WriteResult(path, sha1, "Failed Write")


@dask.delayed(name="sha1-digest")
def _sha1_digest(*write_results):
    lines = []
    for wr in write_results:
        if wr.error is not None:
            raise IOError(f"Failed to write for: {wr.path}")
        file = wr.path.split("/")[-1]
        lines.append(f"{wr.sha1}\t{file}\n")
    return "".join(lines)


class S3COGSink:
    def __init__(
        self,
        creds: Union[ReadOnlyCredentials, str, None] = None,
        cog_opts: Optional[Dict[str, Any]] = None,
        acl: Optional[str] = None,
        public: bool = False,
    ):
        """
        :param creds: S3 write credentials
        :param cog_opts: Configure compression settings, globally and per-band
        :param acl: Canned ACL string:
                    private|public-read|public-read-write|authenticated-read|
                    aws-exec-read|bucket-owner-read|bucket-owner-full-control
        :param public: Mark objects as public access, same as `acl="public-read"`

        Example of COG config

        .. code-block:: python

           # - Lossless compression for all bands but rgba
           # - Webp compression for rgba band
           cfg = {
               "compression": "deflate",
               "zlevel": 9,
               "blocksize": 1024,
               "overrides": {"rgba": {"compression": "webp", "webp_level": 80}},
           }


        """

        if cog_opts is None:
            cog_opts = dict(**DEFAULT_COG_OPTS)
        else:
            tmp = dict(**DEFAULT_COG_OPTS)
            tmp.update(cog_opts)
            cog_opts = tmp

        cog_opts_per_band = cast(
            Dict[str, Dict[str, Any]], cog_opts.pop("overrides", {})
        )
        per_band_cfg = {k: v for k, v in cog_opts.items() if isinstance(v, dict)}
        if per_band_cfg:
            for k in per_band_cfg:
                cog_opts.pop(k)
            cog_opts_per_band.update(per_band_cfg)

        if acl is None and public:
            acl = "public-read"

        self._creds = creds
        self._cog_opts = cog_opts
        self._cog_opts_per_band = cog_opts_per_band
        self._stac_meta_ext = "stac-item.json"
        self._odc_meta_ext = "odc-metadata.yaml"
        self._proc_info_ext = "proc-info.yaml"
        self._stac_meta_contentype = "application/json"
        self._odc_meta_contentype = "text/yaml"
        self._prod_info_meta_contentype = "text/yaml"
        self._band_ext = EXT_TIFF
        self._acl = acl

    def uri(self, task: Task) -> str:
        return task.metadata_path("absolute", ext=self._stac_meta_ext)

    def _get_creds(self) -> ReadOnlyCredentials:
        if self._creds is None:
            self._creds = load_creds()
        if isinstance(self._creds, str):
            self._creds = load_creds(self._creds)
        return self._creds

    def verify_s3_credentials(self, test_uri: Optional[str] = None) -> bool:
        try:
            _ = self._get_creds()
        except ValueError:
            return False
        if test_uri is None:
            return True
        rr = self._write_blob(b"verifying S3 permissions", test_uri).compute()
        assert rr.path == test_uri
        return rr.error is None

    def _write_blob(
        self, data, url: str, ContentType: Optional[str] = None, with_deps=None
    ) -> Delayed:
        """
        Returns Delayed WriteResult[path, sha1, error=None]
        """
        _u = urlparse(url)
        sha1 = _dask_sha1(data)

        if _u.scheme == "s3":
            kw = dict(creds=self._get_creds())
            if ContentType is not None:
                kw["ContentType"] = ContentType
            if self._acl is not None:
                kw["ACL"] = self._acl

            return _pack_write_result(
                save_blob_to_s3(data, url, with_deps=with_deps, **kw), sha1
            )
        elif _u.scheme == "file":
            _dir = Path(_u.path).parent
            if not _dir.exists():
                _dir.mkdir(parents=True, exist_ok=True)
            return _pack_write_result(
                save_blob_to_file(data, _u.path, with_deps=with_deps), sha1
            )
        else:
            raise ValueError(f"Don't know how to save to '{url}'")

    def _ds_to_cog(self, ds: xr.Dataset, paths: Dict[str, str]) -> List[Delayed]:
        out = []
        for band, dv in ds.data_vars.items():
            band = str(band)
            url = paths.get(band, None)
            if url is None:
                raise ValueError(f"No path for band: '{band}'")
            cog_opts = self.cog_opts(band)
            cog_bytes = to_cog(dv, **cog_opts)
            out.append(self._write_blob(cog_bytes, url, ContentType="image/tiff"))
        return out

    def _apply_color_ramp(self, ds: xr.Dataset, ows_style_dict: dict, task: Task):
        from datacube_ows.styles.api import StandaloneStyle
        from datacube_ows.styles.api import apply_ows_style

        ows_style = StandaloneStyle(ows_style_dict)
        # assign the time to xr.Dataset cause ows needs it
        time_da = xr.Dataset({"time": task.time_range.start})
        dst = ds.expand_dims(time=time_da.to_array())
        return apply_ows_style(ows_style, dst)


    def _get_thumbnail(self, ds: xr.Dataset, task: Task, ows_style_dict: dict, input_geobox: GridSpec, odc_file_path: str) -> Delayed:
        display_pixels = []

        image = self._apply_color_ramp(ds, ows_style_dict, task)
        # apply the OWS styling, the return image must have red, green, blue and alpha.
        for display_band in ['red', 'green', 'blue']:
            display_pixels.append(image[display_band].values.reshape([task.geobox.shape[0], task.geobox.shape[1]]))

        thumbnail_bytes = FileWrite().create_thumbnail_from_numpy(rgb=display_pixels,
                                                                  static_stretch=(0, 255),
                                                                  out_scale=10,
                                                                  input_geobox=input_geobox,
                                                                  nodata=None)

        return self._write_blob(thumbnail_bytes, odc_file_path.split('.')[0] + f"_thumbnail.jpg", ContentType="image/jpeg")

    def _ds_to_thumbnail_cog(self, ds: xr.Dataset, task: Task) -> List[Delayed]:
        odc_file_path = task.metadata_path("absolute", ext=self._odc_meta_ext)

        thumbnail_cogs = []

        input_geobox = GridSpec(shape=task.geobox.shape, 
                                transform=task.geobox.transform, 
                                crs=CRS.from_epsg(task.geobox.crs.to_epsg()))

        if task.product.preview_image_ows_style:
            try:
                thumbnail_cog = self._get_thumbnail(ds, task, task.product.preview_image_ows_style, input_geobox, odc_file_path)
                thumbnail_cogs.append(thumbnail_cog)
            except AttributeError:
                _log.error(f"Cannot parse OWS styling: {task.product.preview_image_ows_style}.")
            except ImportError as e:
                raise type(e)(str(e) + '. Please run python -m pip install "odc-stats[ows]" to setup environment to generate thumbnail.')

        return thumbnail_cogs

    def cog_opts(self, band_name: str = "") -> Dict[str, Any]:
        opts = dict(self._cog_opts)
        opts.update(self._cog_opts_per_band.get(band_name, {}))
        return opts

    def write_cog(self, da: xr.DataArray, url: str) -> Delayed:
        cog_bytes = to_cog(da, **self.cog_opts(str(da.name)))
        return self._write_blob(cog_bytes, url, ContentType="image/tiff")

    def exists(self, task: Union[Task, str]) -> bool:
        if isinstance(task, str):
            uri = task
        else:
            uri = self.uri(task)
        _u = urlparse(uri)
        if _u.scheme == "s3":
            s3 = s3_client(creds=self._get_creds(), cache=True)
            meta = s3_head_object(uri, s3=s3)
            return meta is not None
        elif _u.scheme == "file":
            return Path(_u.path).exists()
        else:
            raise ValueError(f"Can't handle url: {uri}")

    def get_eo3_stac_meta(self, task: Task, meta: DatasetDoc, stac_file_path: str, odc_file_path: str) -> str:
        """
        Convert the eodatasets3 DatasetDoc to stac meta format string.
        The stac_meta is Python dict, please use json_fallback() to format it. Also pass dataset_location
        to convert all accessories to full url. The S3 and local dir will use different ways to extract.
        """
        _u = urlparse(stac_file_path)

        if _u.scheme == "s3":
            dataset_location = f"{_u.scheme}://{_u.netloc}/{'/'.join(_u.path.split('/')[:-1])}"
        else:
            dataset_location = str(Path(_u.path).parent)

        stac_meta = eo3stac.to_stac_item(dataset=meta,
                                        stac_item_destination_url=stac_file_path,
                                        dataset_location=dataset_location,
                                        odc_dataset_metadata_url =odc_file_path,
                                        explorer_base_url = task.product.explorer_path
                                        )
        return json.dumps(stac_meta, default=json_fallback) # stac_meta is Python str, but content is 'Dict format'

    def dump_with_pystac(self, task: Task, ds: Dataset, aux: Optional[Dataset] = None) -> Delayed:
        """
        Dump files with STAC metadata file, which generated from PySTAC
        """
        json_url = task.metadata_path("absolute", ext=self._stac_meta_ext)
        meta = task.render_metadata(ext=self._band_ext)
        json_data = dump_json(meta).encode("utf8")

        # fake write result for metadata output, we want metadata file to be
        # the last file written, so need to delay it until after sha1 is
        # written.
        meta_sha1 = dask.delayed(WriteResult(json_url, mk_sha1(json_data), None))

        paths = task.paths("absolute", ext=self._band_ext)
        cogs = self._ds_to_cog(ds, paths)

        if aux is not None:
            aux_paths = {
                k: task.aux_path(k, relative_to="absolute", ext=self._band_ext)
                for k in aux.data_vars
            }
            cogs.extend(self._ds_to_cog(aux, aux_paths))

        # this will raise IOError if any write failed, hence preventing json
        # from being written
        sha1_digest = _sha1_digest(meta_sha1, *cogs)
        sha1_url = task.metadata_path("absolute", ext="sha1")
        sha1_done = self._write_blob(sha1_digest, sha1_url, ContentType="text/plain")

        return self._write_blob(
            json_data, json_url, ContentType=self._stac_meta_contentype, with_deps=sha1_done,
        )

    def dump_with_eodatasets3(self, task: Task, ds: Dataset, aux: Optional[Dataset] = None, proc: StatsPluginInterface = None) -> Delayed:
        """
        Dump files with metadata files, which generated from eodatasets3
        """
        stac_file_path = task.metadata_path("absolute", ext=self._stac_meta_ext)
        odc_file_path = task.metadata_path("absolute", ext=self._odc_meta_ext)
        sha1_url = task.metadata_path("absolute", ext="sha1")
        proc_info_url = task.metadata_path("absolute", ext=self._proc_info_ext)
        dataset_assembler = task.render_assembler_metadata(ext=self._band_ext, output_dataset=ds)

        dataset_assembler.extend_user_metadata("input-products", sorted(set([e.type.name for e in task.datasets])))

        dataset_assembler.extend_user_metadata("odc-stats-config", vars(task.product))

        dataset_assembler.note_software_version("eodatasets3",
                                                "https://github.com/GeoscienceAustralia/eo-datasets",
                                                eodatasets3.__version__,)

        dataset_assembler.note_software_version('odc-stats',
                                                "https://github.com/opendatacube/odc-tools",
                                                __version__)

        dataset_assembler.note_software_version(proc.NAME,
                                                "https://github.com/opendatacube/odc-tools",
                                                proc.VERSION)

        if task.product.preview_image_ows_style:
            try:
                import datacube_ows
                thumbnail_path = odc_file_path.split('.')[0] + f"_thumbnail.jpg"
                dataset_assembler._accessories["thumbnail"] = Path(urlparse(thumbnail_path).path).name

                dataset_assembler.note_software_version("datacube-ows",
                                                        "https://github.com/opendatacube/datacube-ows",
                                                        # Just realized the odc-stats does not have version.
                                                        datacube_ows.__version__)
            except ImportError as e:
                raise type(e)(str(e) + '. Please run python -m pip install "odc-stats[ows]" to setup environment to generate thumbnail.')

        dataset_assembler._accessories["checksum:sha1"] = Path(urlparse(sha1_url).path).name
        dataset_assembler._accessories["metadata:processor"] = Path(urlparse(proc_info_url).path).name

        meta = dataset_assembler.to_dataset_doc()
        # already add all information to dataset_assembler, now convert to odc and stac metadata format

        stac_meta = self.get_eo3_stac_meta(task, meta, stac_file_path, odc_file_path)

        odc_meta_stream = io.StringIO("") # too short, not worth to move to another method.
        serialise.to_stream(odc_meta_stream, meta)
        odc_meta = odc_meta_stream.getvalue() # odc_meta is Python str

        proc_info_meta_stream = io.StringIO("")
        serialise._init_yaml().dump({**dataset_assembler._user_metadata, "software_versions": dataset_assembler._software_versions}, proc_info_meta_stream)
        proc_info_meta = proc_info_meta_stream.getvalue()

        # fake write result for metadata output, we want metadata file to be
        # the last file written, so need to delay it until after sha1 files is
        # written.
        stac_meta_sha1 = dask.delayed(WriteResult(stac_file_path, mk_sha1(stac_meta), None))
        odc_meta_sha1 = dask.delayed(WriteResult(odc_file_path, mk_sha1(odc_meta), None))
        proc_info_sha1 = dask.delayed(WriteResult(proc_info_url, mk_sha1(proc_info_meta), None))

        paths = task.paths("absolute", ext=self._band_ext)
        cogs = self._ds_to_cog(ds, paths)

        if aux is not None:
            aux_paths = {
                k: task.aux_path(k, relative_to="absolute", ext=self._band_ext)
                for k in aux.data_vars
            }
            cogs.extend(self._ds_to_cog(aux, aux_paths))

        thumbnail_cogs = self._ds_to_thumbnail_cog(ds, task)

        # this will raise IOError if any write failed, hence preventing json
        # from being written
        sha1_digest = _sha1_digest(stac_meta_sha1, odc_meta_sha1, proc_info_sha1, *cogs, *thumbnail_cogs)
        sha1_done = self._write_blob(sha1_digest, sha1_url, ContentType="text/plain")

        proc_info_done = self._write_blob(proc_info_meta, proc_info_url, ContentType=self._prod_info_meta_contentype, with_deps=sha1_done)
        odc_meta_done = self._write_blob(odc_meta, odc_file_path, ContentType=self._odc_meta_contentype, with_deps=proc_info_done)
        cog_done = self._write_blob(stac_meta, stac_file_path, ContentType=self._stac_meta_contentype, with_deps=odc_meta_done)

        # The uploading DAG is:
        # sha1_done -> proc_info_done -> odc_meta_done -> stac_meta_done
        return cog_done

    def dump(self, task: Task, ds: Dataset, aux: Optional[Dataset] = None, proc: StatsPluginInterface = None, apply_eodatasets3: Optional[bool] = False) -> Delayed:
        
        if apply_eodatasets3:
            return self.dump_with_eodatasets3(task, ds, aux, proc)
        else:
            return self.dump_with_pystac(task, ds, aux)