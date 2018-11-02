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


class S3Fetcher(object):
    def __init__(self,
                 nconcurrent=24,
                 nthreads=1,
                 region_name=None,
                 max_buffer=1000,
                 addressing_style='path'):
        from ..io.async import AsyncWorkerPool
        from aiobotocore.config import AioConfig

        if region_name is None:
            region_name = auto_find_region()

        s3_cfg = AioConfig(max_pool_connections=nconcurrent,
                           s3=dict(addressing_style=addressing_style))

        self._pool = AsyncWorkerPool(nthreads=nthreads,
                                     tasks_per_thread=nconcurrent,
                                     max_buffer=max_buffer)
        self._tls = threading.local()
        self._closed = False

        async def setup(tls):
            tls.session = aiobotocore.get_session()
            tls.s3 = tls.session.create_client('s3',
                                               region_name=region_name,
                                               config=s3_cfg)
            return (tls.session, tls.s3)

        self._threads_state = self._pool.broadcast(setup, self._tls)

    def close(self):
        async def _close(tls):
            await tls.s3.close()

        if not self._closed:
            self._pool.broadcast(_close, self._tls)
            self._closed = True

    def __del__(self):
        self.close()

    def _worker(self, url):
        if isinstance(url, tuple):
            url, range = url
        else:
            range = None

        return s3_fetch_object(url, s3=self._tls.s3, range=range)

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
