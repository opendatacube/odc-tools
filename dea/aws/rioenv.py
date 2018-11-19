"""Helper module for dealing with AWS AIM credentials and rasterio S3 access.

"""
import logging
import threading
import rasterio
from botocore.credentials import ReadOnlyCredentials
import botocore.session
from types import SimpleNamespace

from . import auto_find_region, get_creds_with_retry


_thread_lcl = threading.local()

_default_env = SimpleNamespace(creds=None,
                               lock=threading.Lock())

log = logging.getLogger(__name__)


def aws_session_env(frozen_credentials, region_name=None):
    c = frozen_credentials
    ee = dict(AWS_ACCESS_KEY_ID=c.access_key,
              AWS_SECRET_ACCESS_KEY=c.secret_key)
    if c.token:
        ee['AWS_SESSION_TOKEN'] = c.token
    if region_name:
        ee['AWS_REGION'] = region_name
    return ee


class SimpleSession(rasterio.session.Session):
    def __init__(self, creds):
        self._creds = creds

    def update(self, **creds):
        self._creds.update(**creds)

    def get_credential_options(self):
        return self._creds


class AWSRioEnv(object):
    """This class is needed to overcome limitation in rasterio AWSSession.

       AWSSession assumes that credentials are valid for the duration of the
       environment, but we need to renew credentials when they expire. However
       creating and activating GDAL environment is relatively expensive
       (several ms even when boto3 sessions is maintained externally), so
       doing that on every read is not ideal, especially since we need extreme
       levels of concurrency (40+ threads).

    """

    @staticmethod
    def _mk_env(*args, **kw):
        env = rasterio.env.Env(*args, **kw)
        env.__enter__()
        return env

    def __init__(self, credentials, region_name=None, **gdal_opts):
        assert credentials is not None

        self._region_name = region_name
        self._creds = credentials
        self._frozen_creds = self._creds.get_frozen_credentials()

        self._creds_session = SimpleSession(aws_session_env(self._frozen_creds, region_name))

        # We activate main environment for the duration of the thread
        self._env_main = AWSRioEnv._mk_env(**gdal_opts)
        # This environment will be redone every time credentials need changing
        self._env_creds = AWSRioEnv._mk_env(session=self._creds_session)

    def clone(self):
        return AWSRioEnv(self._creds, region_name=self._region_name, **self._env_main.options)

    def _needs_refresh(self):
        if isinstance(self._frozen_creds, ReadOnlyCredentials):
            return False

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
            self._creds_session.update(**aws_session_env(self._frozen_creds))
            self._env_creds.__exit__(None, None, None)
            self._env_creds.__enter__()

        return self

    def __exit__(self, type=None, value=None, tb=None):
        pass


def s3_gdal_opts(max_header_sz_kb=None, verbose_curl=None, **extra):
    """Construct dictionary of GDAL parameters needed for efficient reading of
       COGs from S3.

    max_header_sz_kb -- Hint GDAL how many bytes to fetch on open before
                        parsing header, needed if your files header doesn't fit
                        into 16K chunk GDAL fetches by default.

    verbose_curl -- log a lot of info to stderr from curl (useful when
                    debugging performance issues)

    **extra -- Any other GDAL options or overrides
    """
    opts = dict(VSI_CACHE=True,
                CPL_VSIL_CURL_ALLOWED_EXTENSIONS='tif',
                GDAL_DISABLE_READDIR_ON_OPEN='EMPTY_DIR')

    if max_header_sz_kb is not None:
        opts.update(GDAL_INGESTED_BYTES_AT_OPEN=max_header_sz_kb*1024)

    if verbose_curl is not None:
        opts.update(CPL_CURL_VERBOSE=verbose_curl)

    opts.update(**extra)

    return opts


def has_local_env():
    """ Check if environment was already configured in this thread
    """
    return getattr(_thread_lcl, 'main_env', None) is not None


def get_credentials():
    """ Thread-safe singleton for botocore.credentials

        Might still return None
    """
    creds = _default_env.creds
    if creds is not None:
        return creds

    with _default_env.lock:
        if _default_env.creds is None:
            _default_env.creds = get_creds_with_retry(botocore.session.get_session())

        return _default_env.creds


def setup_local_env(credentials=None, region_name=None, src_env=None, **kwargs):
    """Has to be called in each worker thread.

       credentials -- botocore credentials to use, if None will use get_credentials
       region_name -- AWS region name

       src_env -- Source environment to clone in the new thread, all other
       arguments are then ignored.


       **kwargs -- are passed to s3_gdal_opts

        See s3_gdal_opts
    """
    current_env = getattr(_thread_lcl, 'main_env', None)
    if current_env is not None:
        log.info('About to replace thread-local GDAL environment')
        current_env.destroy()

    if src_env is not None:
        _thread_lcl.main_env = src_env.clone()
        return _thread_lcl.main_env

    if region_name is None:
        region_name = auto_find_region()

    if credentials is None:
        credentials = get_credentials()
        if credentials is None:
            raise IOError("Failed to obtain AWS credentials after multiple attempts")

    gdal_opts = s3_gdal_opts(**kwargs)
    _thread_lcl.main_env = AWSRioEnv(credentials, region_name=region_name, **gdal_opts)

    return _thread_lcl.main_env


def local_env():
    """ Returns thread-local instance of current AWSRioEnv.

    Have to first call setup_local_env(...) in this thread.
    """
    return _thread_lcl.main_env
