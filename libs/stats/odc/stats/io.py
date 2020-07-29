"""
Various I/O adaptors
"""

from typing import Dict, Any, Optional
import json
from dask.delayed import Delayed

from datacube.utils.aws import get_creds_with_retry, mk_boto_session, s3_client
from odc.aws import s3_head_object  # TODO: move it to datacube
from datacube.utils.dask import save_blob_to_s3
from datacube.utils.cog import to_cog
from datacube.model import Dataset
from botocore.credentials import ReadOnlyCredentials
from .model import Task


DEFAULT_COG_OPTS = dict(
    compress='deflate',
    predict=2,
    zlevel=6,
    blocksize=512,
)


def ds_to_cog(ds: Dataset,
              paths: Dict[str, str],
              creds: ReadOnlyCredentials,
              **cog_opts: Dict[str, Any]):
    out = []
    for band, dv in ds.data_vars.items():
        url = paths.get(band, None)
        if url is None:
            raise ValueError(f"No path for band: '{band}'")
        cog_bytes = to_cog(dv, **cog_opts)
        out.append(save_blob_to_s3(cog_bytes,
                                   url,
                                   creds=creds,
                                   ContentType='image/tiff'))
    return out


def load_creds(profile: Optional[str] = None) -> ReadOnlyCredentials:
    session = mk_boto_session(profile=profile)
    creds = get_creds_with_retry(session)
    if creds is None:
        raise ValueError("Failed to obtain credentials")

    return creds.get_frozen_credentials()


def dump_json(meta: Dict[str, Any]) -> str:
    return json.dumps(meta, separators=(',', ':'))


class S3COGSink:
    def __init__(self,
                 creds: Optional[ReadOnlyCredentials] = None,
                 cog_opts: Optional[Dict[str, Any]] = None):
        if creds is None:
            creds = load_creds()

        if cog_opts is None:
            cog_opts = dict(**DEFAULT_COG_OPTS)

        self._creds = creds
        self._cog_opts = cog_opts
        self._meta_ext = 'json'
        self._meta_contentype = 'application/json'
        self._band_ext = 'tiff'

    def exists(self, task: Task) -> bool:
        uri = task.metadata_path('absolute', ext=self._meta_ext)
        s3 = s3_client(creds=self._creds, cache=True)
        meta = s3_head_object(uri, s3=s3)
        return meta is not None

    def dump(self,
             task: Task,
             ds: Dataset) -> Delayed:
        paths = task.paths('absolute', ext=self._band_ext)
        cogs = ds_to_cog(ds, paths, self._creds, **self._cog_opts)
        json_url = task.metadata_path('absolute', ext=self._meta_ext)
        meta = task.render_metadata(ext=self._band_ext)

        json_txt = dump_json(meta)

        return save_blob_to_s3(json_txt,
                               json_url,
                               creds=self._creds,
                               ContentType=self._meta_contentype,
                               with_deps=cogs)
