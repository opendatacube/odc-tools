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

from datacube.utils.aws import get_creds_with_retry, mk_boto_session, s3_client
from odc.aws import s3_head_object  # TODO: move it to datacube
from datacube.utils.dask import save_blob_to_s3, save_blob_to_file
from datacube.utils.cog import to_cog
from datacube.model import Dataset
from botocore.credentials import ReadOnlyCredentials
from .model import Task, EXT_TIFF
from hashlib import sha1
from collections import namedtuple


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
        self._meta_ext = "stac-item.json"
        self._meta_contentype = "application/json"
        self._band_ext = EXT_TIFF
        self._acl = acl

    def uri(self, task: Task) -> str:
        return task.metadata_path("absolute", ext=self._meta_ext)

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

    def dump(self, task: Task, ds: Dataset, aux: Optional[Dataset] = None) -> Delayed:
        json_url = task.metadata_path("absolute", ext=self._meta_ext)
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
            json_data, json_url, ContentType=self._meta_contentype, with_deps=sha1_done,
        )
