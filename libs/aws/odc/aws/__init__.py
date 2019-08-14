from urllib.parse import urlparse
import botocore
import botocore.session
import logging
import time

log = logging.getLogger(__name__)


def _fetch_text(url, timeout=0.1):
    from urllib.request import urlopen
    try:
        with urlopen(url, timeout=timeout) as resp:
            if 200 <= resp.getcode() < 300:
                return resp.read().decode('utf8')
            else:
                return None
    except IOError:
        return None


def ec2_metadata(timeout=0.1):
    import json
    txt = _fetch_text('http://169.254.169.254/latest/dynamic/instance-identity/document', timeout)

    if txt is None:
        return None

    try:
        return json.loads(txt)
    except json.JSONDecodeError:
        return None


def ec2_current_region():
    cfg = ec2_metadata()
    if cfg is None:
        return None
    return cfg.get('region', None)


def botocore_default_region(session=None):
    if session is None:
        session = botocore.session.get_session()
    return session.get_config_variable('region')


def auto_find_region(session=None):
    region_name = botocore_default_region(session)

    if region_name is None:
        region_name = ec2_current_region()

    if region_name is None:
        raise ValueError('Region name is not supplied and default can not be found')

    return region_name


def make_s3_client(region_name=None,
                   max_pool_connections=32,
                   session=None,
                   profile=None,
                   use_ssl=True):
    if session is None:
        if profile is None:
            session = botocore.session.get_session()
        else:
            session = botocore.session.Session(profile=profile)

    if region_name is None:
        region_name = auto_find_region(session)

    protocol = 'https' if use_ssl else 'http'

    s3 = session.create_client('s3',
                               region_name=region_name,
                               endpoint_url='{}://s3.{}.amazonaws.com'.format(protocol, region_name),
                               config=botocore.client.Config(max_pool_connections=max_pool_connections))
    return s3


def s3_url_parse(url):
    """ Return Bucket, Key tuple
    """
    uu = urlparse(url)
    return uu.netloc, uu.path.lstrip('/')


def s3_fmt_range(range):
    """ None -> None
        (in, out) -> "bytes={in}-{out-1}"
    """
    if range is None:
        return None

    _in, _out = range
    return 'bytes={:d}-{:d}'.format(_in, _out-1)


def s3_ls(url, s3=None):
    bucket, prefix = s3_url_parse(url)

    s3 = s3 or make_s3_client()
    paginator = s3.get_paginator('list_objects_v2')

    n_skip = len(prefix)
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for o in page.get('Contents', []):
            yield o['Key'][n_skip:]


def s3_ls_dir(uri, s3=None):
    bucket, prefix = s3_url_parse(uri)

    if len(prefix) > 0 and not prefix.endswith('/'):
        prefix = prefix + '/'

    s3 = s3 or make_s3_client()
    paginator = s3.get_paginator('list_objects_v2')

    for page in paginator.paginate(Bucket=bucket,
                                   Prefix=prefix,
                                   Delimiter='/'):
        sub_dirs = page.get('CommonPrefixes', [])
        files = page.get('Contents', [])

        for p in sub_dirs:
            yield 's3://{bucket}/{path}'.format(bucket=bucket, path=p['Prefix'])

        for o in files:
            yield 's3://{bucket}/{path}'.format(bucket=bucket, path=o['Key'])


def s3_find(url, pred=None, glob=None, s3=None):
    """ List all objects under certain path

        each s3 object is represented by a SimpleNamespace with attributes:
        - url
        - size
        - last_modified
        - etag
    """
    from ._find import norm_predicate, s3_file_info

    if glob is None and isinstance(pred, str):
        pred, glob = None, pred

    pred = norm_predicate(pred, glob)

    if url[-1] != '/':
        url += '/'

    bucket, prefix = s3_url_parse(url)

    s3 = s3 or make_s3_client()
    paginator = s3.get_paginator('list_objects_v2')

    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for o in page.get('Contents', []):
            o = s3_file_info(o, bucket)
            if pred is not None and pred(o):
                yield o


def get_boto_session(region_name=None, cache=None, profile=None):
    """ Get botocore.session with correct region_name configured
    """
    if cache is not None:
        sessions = getattr(cache, 'sessions', None)
        if sessions is None:
            sessions = {}
            setattr(cache, 'sessions', sessions)

        session = sessions.get(region_name)
    else:
        sessions, session = {}, None

    if session is None:
        session = botocore.session.Session(profile=profile)
        _region = session.get_config_variable("region")

        if _region is None:
            if region_name is None or region_name == "auto":
                _region = auto_find_region(session)
            else:
                _region = region_name
            session.set_config_variable("region", _region)

        sessions[_region] = session

    return session


def get_creds_with_retry(session, max_tries=10, sleep=0.1):
    for i in range(max_tries):
        if i > 0:
            time.sleep(sleep)
            sleep = min(sleep*2, 10)

        creds = session.get_credentials()
        if creds is not None:
            return creds

    return None


def s3_fetch(url, s3=None, **kwargs):
    s3 = s3 or make_s3_client()
    bucket, key = s3_url_parse(url)
    oo = s3.get_object(Bucket=bucket, Key=key, **kwargs)
    return oo['Body'].read()


def this_instance(ec2=None):
    """ Get dictionary of parameters describing current instance
    """
    info = ec2_metadata()
    if info is None:
        return None

    iid = info.get('instanceId')

    if iid is None:
        return None

    if ec2 is None:
        session = get_boto_session()
        if session is None:
            return None
        ec2 = session.create_client('ec2')

    rr = ec2.describe_instances(InstanceIds=[iid])
    return rr['Reservations'][0]['Instances'][0]


def ec2_tags(ec2=None):
    """ Get tags of the current EC2 instance

        returns None if not running on EC2
        returns Str->Str dictionary of tag name:value
    """
    instance = this_instance(ec2=ec2)
    if instance is None:
        return None

    return {x['Key']: x['Value'] for x in instance.get('Tags', [])}


def read_ssm_params(params, ssm=None):
    """Build dictionary from SSM keys to values in the paramater store.
    """
    if ssm is None:
        ssm = get_boto_session().create_client('ssm')

    result = ssm.get_parameters(Names=[s for s in params],
                                WithDecryption=True)
    failed = result.get('InvalidParameters')
    if failed:
        raise ValueError('Failed to lookup some keys: ' + ','.join(failed))
    return {x['Name']: x['Value']
            for x in result['Parameters']}
