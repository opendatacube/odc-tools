import requests
import re
from urllib.parse import urlparse
import botocore
import botocore.session


def ec2_metadata(timeout=0.1):
    try:
        with requests.get('http://169.254.169.254/latest/dynamic/instance-identity/document', timeout=timeout) as resp:
            if resp.ok:
                return resp.json()
            return None
    except IOError:
        return None


def ec2_current_region():
    cfg = ec2_metadata()
    if cfg is None:
        return None
    return cfg.get('region', None)


def botocore_default_region():
    import botocore.session
    return botocore.session.get_session().get_config_variable('region')


def auto_find_region():
    region_name = ec2_current_region()

    if region_name is None:
        region_name = botocore_default_region()

    if region_name is None:
        raise ValueError('Region name is not supplied and default can not be found')

    return region_name


def make_s3_client(region_name=None,
                   max_pool_connections=32,
                   session=None,
                   use_ssl=True):
    if region_name is None:
        region_name = auto_find_region()

    protocol = 'https' if use_ssl else 'http'

    if session is None:
        session = botocore.session.get_session()

    s3 = session.create_client('s3',
                               region_name=region_name,
                               endpoint_url='{}://s3.{}.amazonaws.com'.format(protocol, region_name),
                               config=botocore.client.Config(max_pool_connections=max_pool_connections))
    return s3


def s3_url_parse(url):
    uu = urlparse(url)
    return uu.netloc, uu.path.lstrip('/')


def s3_ls(url, s3=None):
    bucket, prefix = s3_url_parse(url)

    s3 = s3 or make_s3_client()
    paginator = s3.get_paginator('list_objects')

    n_skip = len(prefix)
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for o in page['Contents']:
            yield o['Key'][n_skip:]


def s3_ls_dir(uri, s3=None):
    bucket, prefix = s3_url_parse(uri)
    prefix = prefix.rstrip('/') + '/'

    s3 = s3 or make_s3_client()
    paginator = s3.get_paginator('list_objects_v2')

    for page in paginator.paginate(Bucket=bucket,
                                   Prefix=prefix,
                                   Delimiter='/'):
        if 'CommonPrefixes' not in page:
            raise ValueError('No such path: ' + uri)

        for p in page['CommonPrefixes']:
            yield 's3://{bucket}/{path}'.format(bucket=bucket, path=p['Prefix'])


def s3_fancy_ls(url, sort=True,
                random_prefix_length=None,
                absolute=False,
                predicate=None,
                s3=None):
    """
    predicate -- None| str -> Bool | regex string
    random_prefix_length int -- number of characters to skip for sorting: fh4e6_0, ahfe8_1 ... 00aa3_9, if =6
    """
    def get_sorter():
        if random_prefix_length is None:
            return None
        return lambda s: s[random_prefix_length:]

    def normalise_predicate(predicate):
        if predicate is None:
            return None

        if isinstance(predicate, str):
            regex = re.compile(predicate)
            return lambda s: regex.match(s) is not None

        return predicate

    predicate = normalise_predicate(predicate)

    if url[-1] != '/':
        url += '/'

    names = s3_ls(url, s3=s3)

    if predicate:
        names = [n for n in names if predicate(n)]

    if sort:
        names = sorted(names, key=get_sorter())

    if absolute:
        names = [url+name for name in names]

    return names


def get_boto3_session(region_name=None, cache=None):
    import boto3

    if region_name is None:
        region_name = auto_find_region()

    if cache is not None:
        sessions = getattr(cache, 'sessions', None)
        if sessions is None:
            sessions = {}
            setattr(cache, 'sessions', sessions)

        session = sessions.get(region_name)
    else:
        sessions, session = {}, None

    if session is None:
        session = boto3.Session(region_name=region_name)
        sessions[region_name] = session

    return session


def s3_get_object_request_maker(region_name=None, credentials=None, ssl=True):
    from botocore.session import get_session
    from botocore.auth import S3SigV4Auth
    from botocore.awsrequest import AWSRequest
    from urllib.request import Request

    session = get_session()

    if region_name is None:
        region_name = auto_find_region()

    if credentials is None:
        credentials = session.get_credentials().get_frozen_credentials()

    protocol = 'https' if ssl else 'http'
    auth = S3SigV4Auth(credentials, 's3', region_name)

    def build_request(bucket=None,
                      key=None,
                      url=None,
                      Range=None):
        if key is None and url is None:
            if bucket is None:
                raise ValueError('Have to supply bucket,key or url')
            # assume bucket is url
            url = bucket

        if url is not None:
            bucket, key = s3_url_parse(url)

        if isinstance(Range, (tuple, list)):
            Range = 'bytes={}-{}'.format(Range[0], Range[1]-1)

        headers = {}
        if Range is not None:
            headers['Range'] = Range

        req = AWSRequest(method='GET',
                         url='{}://s3.{}.amazonaws.com/{}/{}'.format(protocol, region_name, bucket, key),
                         headers=headers)

        auth.add_auth(req)

        return Request(req.url,
                       headers=dict(**req.headers),
                       method='GET')

    return build_request
