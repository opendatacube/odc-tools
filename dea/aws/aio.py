import aiobotocore
import threading
from types import SimpleNamespace

from . import auto_find_region, s3_url_parse, s3_fmt_range


async def s3_fetch_object(url, s3, range=None):
    """ returns object with

     On success:
        .url = url
        .data = bytes
        .last_modified -- last modified timestamp
        .range = None | (in,out)
        .error = None

    On failure:
        .url = url
        .data = None
        .last_modified = None
        .range = None | (in, out)
        .error = str| botocore.Exception class
    """
    from botocore.exceptions import ClientError, BotoCoreError

    def result(data=None, last_modified=None, error=None):
        return SimpleNamespace(url=url, data=data, error=error, last_modified=last_modified, range=range)

    bucket, key = s3_url_parse(url)
    extra_args = {}

    if range is not None:
        try:
            extra_args['Range'] = s3_fmt_range(range)
        except Exception as e:
            return result(error='Bad range passed in: ' + str(range))

    try:
        obj = await s3.get_object(Bucket=bucket, Key=key, **extra_args)
        stream = obj.get('Body', None)
        if stream is None:
            return result(error='Missing Body in response')
        async with stream:
            data = await stream.read()
    except (ClientError, BotoCoreError) as e:
        return result(error=e)

    last_modified = obj.get('LastModified', None)
    return result(data=data, last_modified=last_modified)


def mk_s3_fetcher(region_name=None,
                  max_pool_connections=24,
                  addressing_style='path',
                  session=None):
    from aiobotocore.config import AioConfig

    s3_cfg = AioConfig(max_pool_connections=max_pool_connections,
                       s3=dict(addressing_style=addressing_style))

    if region_name is None:
        region_name = auto_find_region()

    if session is None:
        session = aiobotocore.get_session()

    s3 = session.create_client('s3', region_name=region_name, config=s3_cfg)

    def fetcher(url, range=None):
        return s3_fetch_object(url, s3=s3, range=range)

    return fetcher


class S3Fetcher(object):
    def __init__(self,
                 nconcurrent=24,
                 nthreads=1,
                 region_name=None,
                 addressing_style='path'):
        from ..io.async import AsyncWorkerPool

        if region_name is None:
            region_name = auto_find_region()

        self._fetcher_opts = dict(region_name=region_name,
                                  max_pool_connections=nconcurrent,
                                  addressing_style=addressing_style)

        self._pool = AsyncWorkerPool(nthreads=nthreads, tasks_per_thread=nconcurrent)
        self._tls = threading.local()

    def _fetcher(self):
        """ there is one fetcher for each thread with separate connection pool.
        """
        fetcher = getattr(self._tls, 'fetcher', None)
        if fetcher is None:
            fetcher = mk_s3_fetcher(**self._fetcher_opts)
            self._tls.fetcher = fetcher

        return fetcher

    def _worker(self, url):
        if isinstance(url, tuple):
            url, range = url
        else:
            range = None

        return self._fetcher()(url, range=range)

    def __call__(self, urls):
        """Fetch a bunch of s3 urls concurrently.

        urls -- sequence of  <url | (url, range)> , where range is (in:int,out:int)|None

        On output is a sequence of result objects, note that order is not
        preserved, but one should get one result for every input.

        Successful results object will contain:
          .url = url
          .data = bytes
          .last_modified -- last modified timestamp
          .range = None | (in,out)
          .error = None

        Failed result looks like this:
          .url = url
          .data = None
          .last_modified = None
          .range = None | (in, out)
          .error = str| botocore.Exception class

        """
        return self._pool.map(self._worker, urls)
