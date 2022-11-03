"""
Helper methods for working with AWS
"""
import json
import logging
import os
import threading
import time
from typing import IO, Any, Dict, Optional, Tuple, Union
from urllib.parse import urlparse
from urllib.request import urlopen

import botocore
import botocore.session
from botocore.credentials import Credentials, ReadOnlyCredentials
from botocore.exceptions import ClientError
from botocore.session import Session

from ._find import norm_predicate, s3_file_info

_LCL = threading.local()
ByteRange = Union[slice, Tuple[int, int]]  # pylint: disable=invalid-name
MaybeS3 = Optional[botocore.client.BaseClient]  # pylint: disable=invalid-name


log = logging.getLogger(__name__)


def _fetch_text(url: str, timeout: float = 0.1) -> Optional[str]:
    try:
        with urlopen(url, timeout=timeout) as resp:
            if 200 <= resp.getcode() < 300:
                return resp.read().decode("utf8")
            else:
                return None
    except IOError:
        return None


def s3_url_parse(url: str) -> Tuple[str, str]:
    """Return Bucket, Key tuple"""
    uu = urlparse(url)
    if uu.scheme != "s3":
        raise ValueError("Not a valid s3 url")
    return uu.netloc, uu.path.lstrip("/")


def s3_fmt_range(r: Optional[ByteRange]):
    """None -> None
    (in, out) -> "bytes={in}-{out-1}"
    """
    if r is None:
        return None

    if isinstance(r, slice):
        if r.step not in [1, None]:
            raise ValueError("Can not process decimated slices")
        if r.stop is None:
            raise ValueError("Can not process open ended slices")

        _in = 0 if r.start is None else r.start
        _out = r.stop
    else:
        _in, _out = r

    if _in < 0 or _out < 0:
        raise ValueError("Slice has to be positive")

    return f"bytes={_in:d}-{_out - 1:d}"


def ec2_metadata(timeout: float = 0.1) -> Optional[Dict[str, Any]]:
    """When running inside AWS returns dictionary describing instance identity.
    Returns None when not inside AWS
    """
    txt = _fetch_text(
        "http://169.254.169.254/latest/dynamic/instance-identity/document", timeout
    )

    if txt is None:
        return None

    try:
        return json.loads(txt)
    except json.JSONDecodeError:
        return None


def ec2_current_region() -> Optional[str]:
    """Returns name of the region  this EC2 instance is running in."""
    cfg = ec2_metadata()
    if cfg is None:
        return None
    return cfg.get("region", None)


def botocore_default_region(session: Optional[Session] = None) -> Optional[str]:
    """Returns default region name as configured on the system."""
    if session is None:
        session = botocore.session.get_session()
    return session.get_config_variable("region")


def auto_find_region(
    session: Optional[Session] = None, default: Optional[str] = None
) -> str:
    """
    Try to figure out which region name to use

    1. Region as configured for this/default session
    2. Region this EC2 instance is running in
    3. Value supplied in `default`
    4. raise exception
    """
    region_name = botocore_default_region(session)

    if region_name is None:
        region_name = ec2_current_region()

    if region_name is not None:
        return region_name

    if default is None:
        raise ValueError("Region name is not supplied and default can not be found")

    return default


def get_creds_with_retry(
    session: Session, max_tries: int = 10, sleep: float = 0.1
) -> Optional[Credentials]:
    """Attempt to obtain credentials upto `max_tries` times with back off
    :param session: botocore session, see mk_boto_session
    :param max_tries: number of attempt before failing and returing None
    :param sleep: number of seconds to sleep after first failure (doubles on every consecutive failure)
    """
    for i in range(max_tries):
        if i > 0:
            time.sleep(sleep)
            sleep = min(sleep * 2, 10)

        creds = session.get_credentials()
        if creds is not None:
            return creds

    return None


def mk_boto_session(
    profile: Optional[str] = None,
    creds: Optional[ReadOnlyCredentials] = None,
    region_name: Optional[str] = None,
) -> Session:
    """Get botocore session with correct `region` configured

    :param profile: profile name to lookup
    :param creds: Override credentials with supplied data
    :param region_name: default region_name to use if not configured for a given profile
    """
    session = botocore.session.Session(profile=profile)

    if creds is not None:
        session.set_credentials(creds.access_key, creds.secret_key, creds.token)

    _region = session.get_config_variable("region")
    if _region is None:
        if region_name is None or region_name == "auto":
            _region = auto_find_region(session, default="us-west-2")
        else:
            _region = region_name
        session.set_config_variable("region", _region)

    return session


def _s3_cache_key(
    profile: Optional[str] = None,
    creds: Optional[ReadOnlyCredentials] = None,
    region_name: Optional[str] = None,
    aws_unsigned: bool = False,
    prefix: str = "s3",
) -> str:
    parts = [
        prefix,
        "" if creds is None else creds.access_key,
        "T" if aws_unsigned else "F",
        profile or "",
        region_name or "",
    ]
    return ":".join(parts)


def _mk_s3_client(
    profile: Optional[str] = None,
    creds: Optional[ReadOnlyCredentials] = None,
    region_name: Optional[str] = None,
    session: Optional[Session] = None,
    use_ssl: bool = True,
    **cfg,
) -> botocore.client.BaseClient:
    """Construct s3 client with configured region_name.

    :param profile    : profile name to lookup (only used if session is not supplied)
    :param creds      : Override credentials with supplied data
    :param region_name: region_name to use, overrides session setting
    :param session    : botocore session to use
    :param use_ssl    : Whether to connect via http or https
    :param cfg        : passed on to ``botocore.client.Config(..)``
                        max_pool_connections
                        connect_timeout
                        read_timeout
                        parameter_validation
                        ...
    """
    if session is None:
        session = mk_boto_session(profile=profile, creds=creds, region_name=region_name)

    extras = {}  # type: Dict[str, Any]
    if creds is not None:
        extras.update(
            aws_access_key_id=creds.access_key,
            aws_secret_access_key=creds.secret_key,
            aws_session_token=creds.token,
        )
    if region_name is not None:
        extras["region_name"] = region_name

    return session.create_client(
        "s3", use_ssl=use_ssl, **extras, config=botocore.client.Config(**cfg)
    )


def _aws_unsigned_check_env() -> bool:
    def parse_bool(v: str) -> bool:
        return v.upper() in ("YES", "Y", "TRUE", "T", "1")

    for evar in ("AWS_UNSIGNED", "AWS_NO_SIGN_REQUEST"):
        v = os.environ.get(evar, None)
        if v is not None:
            return parse_bool(v)

    return False


def s3_client(
    profile: Optional[str] = None,
    creds: Optional[ReadOnlyCredentials] = None,
    region_name: Optional[str] = None,
    session: Optional[Session] = None,
    aws_unsigned: Optional[bool] = None,
    use_ssl: bool = True,
    cache: Union[bool, str] = False,
    **cfg,
) -> botocore.client.BaseClient:
    """Construct s3 client with configured region_name.

    :param profile: profile name to lookup (only used if session is not supplied)
    :param creds: Override credentials with supplied data
    :param region_name: region_name to use, overrides session setting
    :param aws_unsigned: Do not use any credentials when accessing S3 resources
    :param session: botocore session to use
    :param use_ssl: Whether to connect via http or https
    :param cache: ``True`` - store/lookup s3 client in thread local cache.
                  ``"purge"`` - delete from cache and return what was there to begin with

    :param cfg: passed on to ``botocore.client.Config(..)``

    """
    if aws_unsigned is None:
        aws_unsigned = _aws_unsigned_check_env()

    if aws_unsigned:
        cfg.update(signature_version=botocore.UNSIGNED)

    if not cache:
        return _mk_s3_client(
            profile,
            creds=creds,
            region_name=region_name,
            session=session,
            use_ssl=use_ssl,
            **cfg,
        )

    _cache = thread_local_cache("__aws_s3_cache", {})

    key = _s3_cache_key(
        profile=profile, region_name=region_name, creds=creds, aws_unsigned=aws_unsigned
    )

    if cache == "purge":
        return _cache.pop(key, None)

    s3 = _cache.get(key, None)

    if s3 is None:
        s3 = _mk_s3_client(
            profile,
            creds=creds,
            region_name=region_name,
            session=session,
            use_ssl=use_ssl,
            **cfg,
        )
        _cache[key] = s3

    return s3


def s3_open(
    url: str,
    s3: MaybeS3 = None,
    range: Optional[ByteRange] = None,  # pylint: disable=redefined-builtin
    **kwargs,
):
    """Open whole or part of S3 object

    :param url: s3://bucket/path/to/object
    :param s3: pre-configured s3 client, see make_s3_client()
    :param range: Byte range to read (first_byte, one_past_last_byte), default is whole object
    :param kwargs: are passed on to ``s3.get_object(..)``
    """
    if range is not None:
        try:
            kwargs["Range"] = s3_fmt_range(range)
        except Exception:
            raise ValueError("Bad range passed in: " + str(range)) from None

    s3 = s3 or s3_client()
    bucket, key = s3_url_parse(url)
    oo = s3.get_object(Bucket=bucket, Key=key, **kwargs)
    return oo["Body"]


def s3_download(
    url: str,
    destination: Optional[str] = None,
    s3: MaybeS3 = None,
    range: Optional[ByteRange] = None,  # pylint: disable=redefined-builtin
    read_chunk_size: int = 10 * (1 << 20),  # 10Mb
    **kwargs,
) -> str:
    """
    Download file from S3 to local storage

    :param url: Source object
    :param destination: Output file name (defaults to object name in current directory)
    :param s3: pre-configured s3 client, see make_s3_client()
    :param range: Byte range to read (first_byte, one_past_last_byte), default is whole object
    :param read_chunk_size: How many bytes to read at a time (default 10Mb)
    :param kwargs: are passed on to ``s3.get_object(..)``

    :returns: destination file path as a string
    """
    if destination is None:
        bucket, key = s3_url_parse(url)
        destination = key.split("/")[-1]

    src = s3_open(url, s3=s3, range=range, **kwargs)
    with open(destination, "wb") as dst:
        for chunk in src.iter_chunks(read_chunk_size):
            dst.write(chunk)

    return destination


def s3_head_object(url: str, s3: MaybeS3 = None, **kwargs) -> Optional[Dict[str, Any]]:
    """
    Head object, return object metadata.

    :param url: s3://bucket/path/to/object
    :param s3: pre-configured s3 client, see make_s3_client()
    :param kwargs: are passed on to ``s3.head_object(..)``
    """
    s3 = s3 or s3_client()
    bucket, key = s3_url_parse(url)

    try:
        oo = s3.head_object(Bucket=bucket, Key=key, **kwargs)
    except ClientError:
        return None

    meta = oo.pop("ResponseMetadata", {})
    code = meta.get("HTTPStatusCode", 0)
    if 200 <= code < 300:
        return oo

    # it actually raises exceptions when http code is in the "fail" range
    return None  # pragma: no cover


def s3_fetch(
    url: str,
    s3: MaybeS3 = None,
    range: Optional[ByteRange] = None,  # pylint: disable=redefined-builtin
    **kwargs,
) -> bytes:
    """Read entire or part of object into memory and return as bytes

    :param url: s3://bucket/path/to/object
    :param s3: pre-configured s3 client, see make_s3_client()
    :param range: Byte range to read (first_byte, one_past_last_byte), default is whole object
    """
    return s3_open(url, s3=s3, range=range, **kwargs).read()


def s3_dump(data: Union[bytes, str, IO], url: str, s3: MaybeS3 = None, **kwargs):
    """Write data to s3 object.

    :param data: bytes to write
    :param url: s3://bucket/path/to/object
    :param s3: pre-configured s3 client, see s3_client()
    :param kwargs: Are passed on to ``s3.put_object(..)``

    ContentType
    ACL
    """

    s3 = s3 or s3_client()
    bucket, key = s3_url_parse(url)

    r = s3.put_object(Bucket=bucket, Key=key, Body=data, **kwargs)
    code = r["ResponseMetadata"]["HTTPStatusCode"]
    return 200 <= code < 300


###########################################################################
# Code above is also in datacube, code below not yet
###########################################################################


def s3_ls(url, s3=None, **kw):
    bucket, prefix = s3_url_parse(url)

    s3 = s3 or s3_client()
    paginator = s3.get_paginator("list_objects_v2")

    n_skip = len(prefix)
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix, **kw):
        for o in page.get("Contents", []):
            yield o["Key"][n_skip:]


def s3_ls_dir(uri, s3=None, **kw):
    bucket, prefix = s3_url_parse(uri)

    if len(prefix) > 0 and not prefix.endswith("/"):
        prefix = prefix + "/"

    s3 = s3 or s3_client()
    paginator = s3.get_paginator("list_objects_v2")

    for page in paginator.paginate(Bucket=bucket, Prefix=prefix, Delimiter="/", **kw):
        sub_dirs = page.get("CommonPrefixes", [])
        files = page.get("Contents", [])

        for p in sub_dirs:
            yield f"s3://{bucket}/{p['Prefix']}"

        for o in files:
            yield f"s3://{bucket}/{o['Key']}"


def s3_find(url, pred=None, glob=None, s3=None, **kw):
    """List all objects under certain path

    each s3 object is represented by a SimpleNamespace with attributes:
    - url
    - size
    - last_modified
    - etag
    """
    if glob is None and isinstance(pred, str):
        pred, glob = None, pred

    pred = norm_predicate(pred, glob)

    if url[-1] != "/":
        url += "/"

    bucket, prefix = s3_url_parse(url)

    s3 = s3 or s3_client()
    paginator = s3.get_paginator("list_objects_v2")

    for page in paginator.paginate(Bucket=bucket, Prefix=prefix, **kw):
        for o in page.get("Contents", []):
            o = s3_file_info(o, bucket)
            if pred is not None and pred(o):
                yield o


def this_instance(ec2=None):
    """Get dictionary of parameters describing current instance"""
    info = ec2_metadata()
    if info is None:
        return None

    iid = info.get("instanceId")

    if iid is None:
        return None

    if ec2 is None:
        session = mk_boto_session()
        if session is None:
            return None
        ec2 = session.create_client("ec2")

    rr = ec2.describe_instances(InstanceIds=[iid])
    return rr["Reservations"][0]["Instances"][0]


def ec2_tags(ec2=None):
    """Get tags of the current EC2 instance

    returns None if not running on EC2
    returns Str->Str dictionary of tag name:value
    """
    instance = this_instance(ec2=ec2)
    if instance is None:
        return None

    return {x["Key"]: x["Value"] for x in instance.get("Tags", [])}


def read_ssm_params(params, ssm=None):
    """Build dictionary from SSM keys to values in the paramater store."""
    if ssm is None:
        ssm = mk_boto_session().create_client("ssm")

    result = ssm.get_parameters(Names=list(params), WithDecryption=True)
    failed = result.get("InvalidParameters")
    if failed:
        raise ValueError("Failed to lookup some keys: " + ",".join(failed))
    return {x["Name"]: x["Value"] for x in result["Parameters"]}


def thread_local_cache(
    name: str, initial_value: Any = None, purge: bool = False
) -> Any:
    """Define/get thread local object with a given name.

    :param name:          name for this cache
    :param initial_value: Initial value if not set for this thread
    :param purge:         If True delete from cache (returning what was there previously)

    Returns
    -------
    value previously set in the thread or `initial_value`
    """
    absent = object()
    cc = getattr(_LCL, name, absent)
    absent = cc is absent

    if absent:
        cc = initial_value

    if purge:
        if not absent:
            delattr(_LCL, name)
    else:
        if absent:
            setattr(_LCL, name, cc)

    return cc
