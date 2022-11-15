from fnmatch import fnmatch
from itertools import takewhile
from types import SimpleNamespace


# TODO: Document these SimpleNamespace objects that are returned!
# Probably by turning them into dataclasses
def s3_file_info(f, bucket):
    url = f"s3://{bucket}/{f.get('Key')}"
    return SimpleNamespace(
        url=url,
        size=f.get("Size"),
        last_modified=f.get("LastModified"),
        etag=f.get("ETag"),
    )


def norm_predicate(pred=None, glob=None):
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

    glob_set = set("*[]?")

    def is_glob(s):
        return bool(glob_set.intersection(set(s)))

    pp = url_query.split("/")
    base = list(takewhile(lambda s: not is_glob(s), pp))

    qq = pp[len(base) :]

    glob, _file, depth = None, None, None

    if len(qq) > 0:
        last = qq.pop()

        if is_glob(last):
            glob = last
        elif last != "":
            _file = last

        qq_set = set(qq)
        if len(qq) == 0:
            depth = 0
        elif qq_set == {"**"}:
            depth = -1
        elif "**" not in qq_set:
            depth = len(qq)
        else:
            raise ValueError(f"Bad query: {url_query}")

    base = "/".join(base)
    base = base.rstrip("/") + "/"

    return SimpleNamespace(base=base, depth=depth, file=_file, glob=glob)
