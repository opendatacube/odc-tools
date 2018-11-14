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
