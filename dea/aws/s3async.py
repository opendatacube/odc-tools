from . import s3_get_object_request_maker, auto_find_region
from .async_tools import p_fetch
from collections import namedtuple

UserData = namedtuple('UserData', ['url', 'idx'])


def fetch_bunch(urls, on_data, nconnections=64, loop=None, region_name=None):
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

    def data_cbk(data, udata, time=None):
        on_data(data, udata.url, idx=udata.idx, time=time)

    p_fetch(map(mk_request, enumerate(urls)),
            data_cbk,
            nconnections=nconnections,
            loop=loop)
