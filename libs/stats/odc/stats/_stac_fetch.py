import json

from odc.aio import S3Fetcher, s3_find_glob
from odc.stac import stac2ds
from pystac.item import Item


def bytes2stac(data):
    if isinstance(data, bytes):
        data = data.decode("utf-8")
    stac_doc = json.loads(data)
    return Item.from_dict(stac_doc)


def s3_fetch_dss(glob, s3=None):
    if s3 is None:
        s3 = S3Fetcher(aws_unsigned=True)

    blobs = s3(o.url for o in s3_find_glob(glob, skip_check=True, s3=s3))
    stacs = (bytes2stac(b.data) for b in blobs)

    return stac2ds(stacs)
