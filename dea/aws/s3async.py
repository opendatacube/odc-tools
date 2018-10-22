from collections import namedtuple
import logging
import sys

from . import s3_get_object_request_maker, auto_find_region
from .async_tools import p_fetch

log = logging.getLogger(__name__)

UserData = namedtuple('UserData', ['url', 'idx'])


def fetch_bunch(urls, on_data, nconnections=16, loop=None, region_name=None):
    """
    on_data callback order is not guaranteed

    on_data(bytes, url, idx=int, time=(t0, t1, t2))
    """

    if region_name is None:
        # TODO: this should be region of the bucket not region of an instance
        region_name = auto_find_region()

    signer = s3_get_object_request_maker(region_name=region_name)

    def mk_request(uu):
        idx, url = uu
        return UserData(url=url, idx=idx), signer(url=url)

    def data_cbk(data, udata, status=-1, headers=None, time=None):
        if 200 <= status < 300:
            on_data(data, udata.url, idx=udata.idx, time=time)
        else:
            log.error("Failed: %s with %d" % (udata.url, status))
            print('S3 FAIL:', status, data.decode('utf8'), file=sys.stderr)

    p_fetch(map(mk_request, enumerate(urls)),
            data_cbk,
            nconnections=nconnections,
            loop=loop)
