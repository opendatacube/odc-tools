"""Helper module for dealing with AWS AIM credentials and rasterio S3 access.

"""
import logging
import threading
import rasterio
import rasterio.env
from rasterio.session import AWSSession

from .s3tools import get_boto3_session

_thread_lcl = threading.local()

log = logging.getLogger(__name__)


class AWSRioEnv(object):
    """This class is needed to overcome limitation in rasterio AWSSession.

       AWSSession assumes that credentials are valid for the duration of the
       environment, but we need to renew credentials when they expire. However
       creating and activating GDAL environment is relatively expensive
       (several ms even when boto3 sessions is maintained externally), so
       doing that on every read is not ideal, especially since we need extreme
       levels of concurrency (40+ threads).

       It's not super clear from boto3 documentation whether same session can
       be shared across threads, so to be safe we create a new boto3 session
       instance for every thread, we might want to re-assess this choice in the
       future.

    """
    def __init__(self, region_name=None, **gdal_opts):
        self._session = get_boto3_session(region_name=region_name, cache=_thread_lcl)
        self._creds = self._session.get_credentials()
        self._frozen_creds = self._creds.get_frozen_credentials()

        # We activate main environment for the duration of the thread
        self._env_main = rasterio.env.Env(**gdal_opts)
        self._env_main.__enter__()

        # This environment will be redone every time credentials need changing
        self._env_creds = rasterio.env.Env(session=AWSSession(session=self._session))
        self._env_creds.__enter__()

    def _needs_refresh(self):
        frozen_creds = self._creds.get_frozen_credentials()
        if frozen_creds is self._frozen_creds:
            return False
        self._frozen_creds = frozen_creds
        return True

    def destroy(self):
        self._env_creds.__exit__(None, None, None)
        self._env_main.__exit__(None, None, None)

    def __enter__(self):
        """This refreshes rasterio environment only when temporary credentials have
        changed.
        """
        if self._needs_refresh():
            log.info('Refreshing credentials')

            # Currently this is the only way to force new credentials to be
            # injected int gdal environment: need to create new AWSSession and new Env
            self._env_creds.__exit__(None, None, None)
            self._env_creds = rasterio.env.Env(session=AWSSession(session=self._session))
            self._env_creds.__enter__()

        return self

    def __exit__(self, type=None, value=None, tb=None):
        pass


def setup_local_env(**kwargs):
    """ Has to be called in each worker thread.
    """
    current_env = getattr(_thread_lcl, 'main_env', None)
    if current_env is not None:
        log.info('About to replace thread-local GDAL environment')
        current_env.destroy()

    _thread_lcl.main_env = AWSRioEnv(**kwargs)


def local_env():
    """ Returns thread-local instance of current AWSRioEnv.

    Have to first call setup_local_env(...) in this thread.
    """
    return _thread_lcl.main_env
