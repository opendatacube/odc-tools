""" Helper tools for using rasterio in AWS environment.
"""
from typing import Optional, Iterable, Callable, Any
from concurrent.futures import ThreadPoolExecutor
import rasterio

from .rioenv import setup_local_env, local_env
from ..ppr import pool_broadcast


class RioWorkerPool(object):
    """Maintains pool of rasterio worker threads configured for efficient S3 access.

       In particular it sets up GDAL environment in every thread, taking care
       of credentials (including sts refresh). It also makes sure that
       environment setup/tear-down doesn't happen for every operation, but only
       when needed for credential refresh, and even then only credential part
       is replaced.

    """

    def __init__(self, nthreads: int,
                 credentials: Optional[Any]=None,
                 region_name: Optional[str]=None,
                 max_header_sz_kb: Optional[int]=None,
                 **gdal_opts):
        """Construct worker pool.

        nthreads -- number of worker threads

        credentials -- botocore credentials to re-use, if None will create new ones
        region_name -- AWS region name, if None will auto-guess

        max_header_sz_kb -- Hint GDAL how many bytes to fetch on open before
                        parsing header, needed if your files header doesn't fit
                        into 16K chunk GDAL fetches by default.

        **gdal_opts -- Any other GDAL options or overrides

        """

        self._main_env = setup_local_env(credentials, region_name,
                                         max_header_sz_kb=max_header_sz_kb,
                                         **gdal_opts)

        self._nthreads = nthreads
        self._pool = ThreadPoolExecutor(max_workers=nthreads,
                                        thread_name_prefix='rio-')

        self._envs = self._run_per_thread_setup()

    def broadcast(self, action: Callable[..., Any],
                  *args, **kwargs):
        """Broadcast action across all worker threads.

        Will run action in every thread exactly once. This method is useful for
        debugging environment setup and for benchmarking.

        returns a list of results one from each thread

        NOTE:

        Typically this is run on an idle worker pool, but if there are any
        tasks running, calling this will effectively "flush" worker queue, i.e.
        this will block until all on-going tasks are finished and replaced with
        warmup action in each thread. Then when these are done results will be
        returned.
        """
        return pool_broadcast(self._pool, action, *args, **kwargs)

    def _run_per_thread_setup(self):
        return self.broadcast(setup_local_env, src_env=self._main_env)

    @staticmethod
    def _wrap_fn(fn: Callable[..., Any], **kwargs) -> Callable[..., Any]:
        def action(url, *args):
            url = rasterio.parse_path(url)
            with local_env():
                with rasterio.DatasetReader(url, sharing=False) as src:
                    return fn(src, *args, **kwargs)

        return action

    def submit(self, fn: Callable[..., Any], url: str, *args, **kwargs) -> Any:
        """Will try to open url with rasterio then call:

              fn(src, *args, **kwargs)

            Returns future handle, .result() will return whatever fn returns,
            or throw an Exception.
        """
        worker = RioWorkerPool._wrap_fn(fn, **kwargs)
        return self._pool.submit(worker, url, *args)

    def map(self, fn: Callable[..., Any],
            urls: Iterable[str],
            *args, **kwargs) -> Iterable[Any]:
        """ Like map but

            1. Concurrently
            2. Each url is replaced with opened rasterio.DatasetReader

            *args -- iterator for extra per url parameters (like normal map)
            **kwargs -- extra named arguments to fn
        """
        worker = RioWorkerPool._wrap_fn(fn, **kwargs)
        return self._pool.map(worker, urls, *args)

    def lazy_map(self, fn: Callable[..., Any],
                 urls: Iterable[str],
                 *args, **kwargs) -> Iterable[Any]:
        """ Like map but

            1. Concurrently
            2. Each url is replaced with opened rasterio.DatasetReader
            3. Returns iterator over futures not values

            *args -- iterator for extra per url parameters (like normal map)
            **kwargs -- extra named arguments to fn
        """
        worker = RioWorkerPool._wrap_fn(fn, **kwargs)
        for url, *aa in zip(urls, *args):
            yield self._pool.submit(worker, url, *aa)

    @property
    def raw(self) -> ThreadPoolExecutor:
        """ Access underlying worker pool directly.
        """
        return self._pool
