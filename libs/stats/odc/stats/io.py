"""
Various I/O adaptors
"""

from typing import Dict, Any, Optional, List, Union, cast
import json
from urllib.parse import urlparse
import logging
import dask
from dask.delayed import Delayed
from pathlib import Path
import xarray as xr
import io
from PIL import Image
import os
import numpy
from rasterio.crs import CRS
from rasterio.enums import Resampling
import rasterio
import tempfile
import pkg_resources

from datacube.utils.aws import get_creds_with_retry, mk_boto_session, s3_client
from odc.aws import s3_head_object  # TODO: move it to datacube
from datacube.utils.dask import save_blob_to_s3, save_blob_to_file
from datacube.utils.cog import to_cog
from datacube.model import Dataset
from botocore.credentials import ReadOnlyCredentials
from .model import Task, EXT_TIFF, StatsPluginInterface
from hashlib import sha1
from collections import namedtuple

from eodatasets3.assemble import DatasetAssembler, serialise
from eodatasets3.scripts.tostac import dc_to_stac, json_fallback
from eodatasets3.model import DatasetDoc
from eodatasets3.images import FileWrite, GridSpec
import eodatasets3.stac as eo3stac

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

    def get_stac_meta(self, task: Task, meta: DatasetDoc, stac_file_path: str, odc_file_path: str) -> str:
        """
        Convert the DatasetDoc to stac meta format string.
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

    def dump(self, task: Task, ds: Dataset, aux: Optional[Dataset] = None, proc: StatsPluginInterface = None) -> Delayed:

        stac_file_path = task.metadata_path("absolute", ext=self._stac_meta_ext)
        odc_file_path = task.metadata_path("absolute", ext=self._odc_meta_ext)
        sha1_url = task.metadata_path("absolute", ext="sha1")
        proc_info_url = task.metadata_path("absolute", ext=self._proc_info_ext)
        dataset_assembler = task.render_metadata(ext=self._band_ext, output_dataset=ds)

        dataset_assembler.note_software_version('odc-stats',
                                                "https://github.com/opendatacube/odc-tools",
                                                # Just realized the odc-stats does not have version.
                                                [e.version for e in pkg_resources.working_set if e.key == 'odc-stats'][0])

        dataset_assembler.note_software_version(proc.NAME,
                                                "https://github.com/opendatacube/odc-tools",
                                                # Just realized the odc-stats does not have version.
                                                proc.VERSION)

        # add accessories files. we add the filename because odc-metadata need the filename, not full path.
        for band, _ in task.paths(ext="tif").items():
            thumbnail_path = odc_file_path.split('.')[0] + f"_{band}_thumbnail.jpg"
            dataset_assembler._accessories[f"thumbnail:{band}"] = Path(urlparse(thumbnail_path).path).name

        dataset_assembler._accessories["checksum:sha1"] = Path(urlparse(sha1_url).path).name
        dataset_assembler._accessories["metadata:processor"] = Path(urlparse(proc_info_url).path).name

        meta = dataset_assembler.to_dataset_doc()
        # already add all information to dataset_assembler, now convert to odc and stac metadata format

        stac_meta = self.get_stac_meta(task, meta, stac_file_path, odc_file_path)

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

        thumbnail_cogs = []

        input_geobox = GridSpec(shape=task.geobox.shape, transform=task.geobox.transform,  crs=CRS.from_epsg(task.geobox.crs.to_epsg()))

        # if we do not define the r/g/b values, we will grab the first avaialble variable shape to generate full zero numpy
        zero_band = numpy.zeros_like(ds[list(ds.keys())[0]].values.reshape([task.geobox.shape[0], task.geobox.shape[1]]))

        if task.product.preview_image_singleband:
            # single_band: {"measurement": "count_clear", "lookup_table": {"0":[150,150,110]}}, or
            # single_band: {"measurement":'frequency', 'bit':1, 'display_bands': ['blue']} or 
            # single_band: {"measurement":'frequency', 'bit':1, 'display_bands': ['red', 'green', 'blue']}
            for single_band in task.product.preview_image_singleband:
                band = single_band['measurement']
                thumbnail_path = odc_file_path.split('.')[0] + f"_{band}_thumbnail.jpg"

                tuning_pixels, stretch = FileWrite()._filter_singleband_data(data=ds[band].values.reshape([task.geobox.shape[0], task.geobox.shape[1]]),
                                                                            bit=single_band['bit'] if 'bit' in single_band else None,
                                                                            lookup_table=single_band['lookup_table'] if 'lookup_table' in single_band else None)
                if 'bit' in single_band:
                    # the bit operation return tuning_pixels is numapy.array
                    if 'display_bands' not in single_band: # if no display_band, duplicate it as r,g,b
                        tuning_pixels = [tuning_pixels, tuning_pixels, tuning_pixels]
                    else:
                        display_pixels = []
                        for display_band in ["red", "green", "blue"]:
                            display_pixels.append(tuning_pixels) if display_band in single_band["display_bands"] else display_pixels.append(zero_band)
                        tuning_pixels = display_pixels # make sure the tuning_pixcels format is [numpy.array, numpy.array, numpy.array] == [r, g, b]

                thumbnail_bytes = FileWrite().create_thumbnail_from_numpy(rgb=tuning_pixels,
                                                                          static_stretch=stretch,
                                                                          input_geobox=input_geobox,
                                                                          nodata=task.product.nodata)
                
                thumbnail_cogs.append(self._write_blob(thumbnail_bytes, thumbnail_path, ContentType="image/jpeg"))
        
        if task.product.preview_image: # change elif to if, so we can dump two kinds of thumbnail
            # single_image: {'thumbnail_name': 'image_1', 'red': 'count_clear', 'green': 'count_wet', 'blue': 'frequency'}}, or
            # single_image: {'thumbnail_name': 'frequency', 'blue': 'frequency'}}
            for single_image in task.product.preview_image:
                display_pixels = []
                for display_band in ['red', 'green', 'blue']:
                    display_pixels.append(ds[single_image[display_band]].values.reshape([task.geobox.shape[0], task.geobox.shape[1]])) if display_band in single_image else display_pixels.append(zero_band)

                thumbnail_name = single_image['thumbnail_name']
                thumbnail_path = odc_file_path.split('.')[0] + f"_{thumbnail_name}_thumbnail.jpg"

                thumbnail_bytes = FileWrite().create_thumbnail_from_numpy(rgb=display_pixels,
                                                                          input_geobox=input_geobox,
                                                                          nodata=task.product.nodata)

                thumbnail_cogs.append(self._write_blob(thumbnail_bytes, thumbnail_path, ContentType="image/jpeg"))
        

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
