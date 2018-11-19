from types import SimpleNamespace


def s3_file_info(f, bucket):
    url = 's3://{}/{}'.format(bucket, f.get('Key'))
    return SimpleNamespace(url=url,
                           size=f.get('Size'),
                           last_modified=f.get('LastModified'),
                           etag=f.get('ETag'))


def norm_predicate(pred=None, glob=None):
    from fnmatch import fnmatch

    def glob_predicate(glob, pred):
        if pred is None:
            return lambda f: fnmatch(f.url, glob)
        else:
            return lambda f: fnmatch(f.url, glob) and pred(f)

    if glob is not None:
        return glob_predicate(glob, pred)

    return pred


def parse_query(url_query):
    """
        - s3://bucket/some/path/
        - s3://bucket/some/path/something
        - s3://bucket/some/path/*/*/
        - s3://bucket/some/path/*/*/file.yaml
        - s3://bucket/some/path/*/*/*.yaml
        - s3://bucket/some/path/**/file.yaml
    """
    from itertools import takewhile

    glob_set = set('*[]?')

    def is_glob(s):
        return bool(glob_set.intersection(set(s)))

    pp = url_query.split('/')
    base = list(takewhile(lambda s: not is_glob(s), pp))

    qq = pp[len(base):]

    glob, _file, depth = None, None, None

    if len(qq) > 0:
        last = qq.pop()

        if is_glob(last):
            glob = last
        elif last != '':
            _file = last

        qq_set = set(qq)
        if len(qq) == 0:
            depth = 0
        elif qq_set == {'**'}:
            depth = -1
        elif qq_set == {'*'}:
            depth = len(qq)
        else:
            raise ValueError('Bad query: %s' % url_query)

    base = '/'.join(base)
    base = base.rstrip('/') + '/'

    return SimpleNamespace(base=base,
                           depth=depth,
                           file=_file,
                           glob=glob)


def test_parse_query():
    import pytest

    E = SimpleNamespace
    base = 's3://bucket/path/a/'

    assert parse_query(base) == E(base=base, depth=None, glob=None, file=None)
    assert parse_query(base+'some') == E(base=base+'some/', depth=None, glob=None, file=None)
    assert parse_query(base+'*') == E(base=base, depth=0, glob='*', file=None)
    assert parse_query(base+'*/*txt') == E(base=base, depth=1, glob='*txt', file=None)
    assert parse_query(base+'*/*/*txt') == E(base=base, depth=2, glob='*txt', file=None)
    assert parse_query(base+'*/*/file.txt') == E(base=base, depth=2, glob=None, file='file.txt')
    assert parse_query(base+'**/*txt') == E(base=base, depth=-1, glob='*txt', file=None)

    with pytest.raises(ValueError):
        parse_query(base+'*/*/something/*yaml')
