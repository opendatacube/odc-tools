import asyncio
import logging
import os
from fnmatch import fnmatch
from types import SimpleNamespace
from typing import Any, Iterator, Optional

import botocore
from aiobotocore.config import AioConfig
from aiobotocore.session import get_session
from botocore.exceptions import BotoCoreError, ClientError
from odc.aws import (
    _aws_unsigned_check_env,
    auto_find_region,
    norm_predicate,
    s3_file_info,
    s3_fmt_range,
    s3_url_parse,
)
from odc.aws._find import parse_query
from odc.ppt import EOS_MARKER, future_results
from odc.ppt.async_thread import AsyncThread


async def _s3_fetch_object(url, s3, _range=None, **kw):
    """returns object with

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

    def result(data=None, last_modified=None, error=None):
        return SimpleNamespace(
            url=url, data=data, error=error, last_modified=last_modified, range=_range
        )

    bucket, key = s3_url_parse(url)
    extra_args = dict(**kw)

    if _range is not None:
        try:
            extra_args["Range"] = s3_fmt_range(_range)
        except Exception:  # pylint:disable=broad-except
            return result(error="Bad range passed in: " + str(_range))

    try:
        obj = await s3.get_object(Bucket=bucket, Key=key, **extra_args)
        stream = obj.get("Body", None)
        if stream is None:
            return result(error="Missing Body in response")
        async with stream:
            data = await stream.read()
    except (ClientError, BotoCoreError) as e:
        return result(error=e)
    except Exception as e:  # pylint:disable=broad-except
        return result(error="Some Error: " + str(e))

    last_modified = obj.get("LastModified", None)
    return result(data=data, last_modified=last_modified)


async def _s3_find_via_cbk(url, cbk, s3, pred=None, glob=None, **kw):
    """List all objects under certain path

    each s3 object is represented by a SimpleNamespace with attributes:
    - url
    - size
    - last_modified
    - etag
    """
    pred = norm_predicate(pred=pred, glob=glob)

    bucket, prefix = s3_url_parse(url)

    if len(prefix) > 0 and not prefix.endswith("/"):
        prefix = prefix + "/"

    pp = s3.get_paginator("list_objects_v2")

    n_total, n = 0, 0

    async for o in pp.paginate(Bucket=bucket, Prefix=prefix, **kw):
        for f in o.get("Contents", []):
            n_total += 1
            f = s3_file_info(f, bucket)
            if pred is None or pred(f):
                n += 1
                await cbk(f)

    return n_total, n


async def s3_find(url, s3, pred=None, glob=None, **kw):
    """List all objects under certain path

    each s3 object is represented by a SimpleNamespace with attributes:
    - url
    - size
    - last_modified
    - etag
    """
    _files = []

    async def on_file(f):
        _files.append(f)

    await _s3_find_via_cbk(url, on_file, s3=s3, pred=pred, glob=glob, **kw)

    return _files


async def s3_head_object(url, s3, **kw):
    """Run head_object return Result or Error

    (Result, None) -- on success
    (None, error) -- on failure

    """

    def unpack(url, rr):
        return SimpleNamespace(
            url=url,
            size=rr.get("ContentLength", 0),
            etag=rr.get("ETag", ""),
            last_modified=rr.get("LastModified"),
            expiration=rr.get("Expiration"),
        )

    bucket, key = s3_url_parse(url)
    try:
        rr = await s3.head_object(Bucket=bucket, Key=key, **kw)
    except (ClientError, BotoCoreError) as e:
        return None, e

    return unpack(url, rr), None


async def s3_dir(url, s3, pred=None, glob=None, **kw):
    """List s3 "directory" without descending into sub directories.

    pred: predicate for file objects file_info -> True|False
    glob: glob pattern for files only

    Returns: (dirs, files)

    where
      dirs -- list of subdirectories in `s3://bucket/path/` format

      files -- list of objects with attributes: url, size, last_modified, etag
    """
    bucket, prefix = s3_url_parse(url)
    pred = norm_predicate(pred=pred, glob=glob)

    if len(prefix) > 0 and not prefix.endswith("/"):
        prefix = prefix + "/"

    pp = s3.get_paginator("list_objects_v2")

    _dirs = []
    _files = []

    async for o in pp.paginate(Bucket=bucket, Prefix=prefix, Delimiter="/", **kw):
        for d in o.get("CommonPrefixes", []):
            d = d.get("Prefix")
            _dirs.append(f"s3://{bucket}/{d}")
        for f in o.get("Contents", []):
            f = s3_file_info(f, bucket)
            if pred is None or pred(f):
                _files.append(f)

    return _dirs, _files


async def s3_dir_dir(url, depth, dst_q, s3, pred=None, **kw):
    """Find directories certain depth from the base, push them to the `dst_q`

    ```
    s3://bucket/a
                 |- b1
                      |- c1/...
                      |- c2/...
                      |- some_file.txt
                 |- b2
                      |- c3/...
    ```

    Given a bucket structure above, calling this function with

    - url s3://bucket/a/
    - depth=1 will produce
         - s3://bucket/a/b1/
         - s3://bucket/a/b2/
    - depth=2 will produce
         - s3://bucket/a/b1/c1/
         - s3://bucket/a/b1/c2/
         - s3://bucket/a/b2/c3/

    Any files are ignored.

    If `pred` is supplied it is expected to be a `str -> bool` mapping, on
    input full path of the sub-directory is given (e.g `a/b1/`) starting from
    root, but not including bucket name. Sub-directory is only traversed
    further if predicate returns True.
    """
    if not url.endswith("/"):
        url = url + "/"

    if depth == 0:
        await dst_q.put(url)
        return

    pp = s3.get_paginator("list_objects_v2")

    async def step(bucket, prefix, depth, work_q, dst_q):

        async for o in pp.paginate(Bucket=bucket, Prefix=prefix, Delimiter="/", **kw):
            for d in o.get("CommonPrefixes", []):
                d = d.get("Prefix")
                if pred is not None and not pred(d):
                    continue

                if depth > 1:
                    await work_q.put((d, depth - 1))
                else:
                    d = f"s3://{bucket}/{d}"
                    await dst_q.put(d)

    bucket, prefix = s3_url_parse(url)
    work_q = asyncio.LifoQueue()
    work_q.put_nowait((prefix, depth))

    while work_q.qsize() > 0:
        _dir, depth = work_q.get_nowait()
        await step(bucket, _dir, depth, work_q, dst_q)


async def s3_walker(url, nconcurrent, s3, guide=None, pred=None, glob=None, **kw):
    """

    guide(url, depth, base) -> 'dir'|'skip'|'deep'
    """

    def default_guide(url, depth, base):
        return "dir"

    if guide is None:
        guide = default_guide

    work_q = asyncio.Queue()
    n_active = 0

    async def step(idx):
        nonlocal n_active

        x = await work_q.get()
        if x is EOS_MARKER:
            return EOS_MARKER

        url, depth, action = x
        depth = depth + 1

        n_active += 1

        _files = []
        if action == "dir":
            _dirs, _files = await s3_dir(url, s3=s3, pred=pred, glob=glob, **kw)

            for d in _dirs:
                action = guide(d, depth=depth, base=url)

                if action != "skip":
                    if action not in ("dir", "deep"):
                        raise ValueError(f"Expect skip|dir|deep got: {action}")

                    work_q.put_nowait((d, depth, action))

        elif action == "deep":
            _files = await s3_find(url, s3=s3, pred=pred, glob=glob)
        else:
            raise RuntimeError(
                f"Expected action to be one of deep|dir but found {action}"
            )

        n_active -= 1

        # Work queue was already empty and we didn't add any more to traverse
        # and no out-standing work is running
        if work_q.empty() and n_active == 0:
            # Tell all workers in the swarm to stop
            for _ in range(nconcurrent):
                work_q.put_nowait(EOS_MARKER)

        return _files

    work_q.put_nowait((url, 0, "dir"))

    return step


class S3Fetcher:
    def __init__(
        self,
        nconcurrent=24,
        region_name=None,
        addressing_style="path",
        aws_unsigned=None,
    ):

        self._closed = True
        if region_name is None:
            region_name = auto_find_region()

        opts = {}
        if aws_unsigned is None:
            aws_unsigned = _aws_unsigned_check_env()

        if aws_unsigned:
            opts["signature_version"] = botocore.UNSIGNED

        s3_cfg = AioConfig(
            max_pool_connections=nconcurrent,
            **opts,
            s3=dict(addressing_style=addressing_style),
        )

        self._nconcurrent = nconcurrent
        self._async = AsyncThread()
        self._s3 = None
        self._s3_ctx = None
        self._session = None

        async def setup(s3_cfg):
            session = get_session()
            s3_ctx = session.create_client(
                "s3",
                region_name=region_name,
                config=s3_cfg,
                endpoint_url=os.environ.get("AWS_S3_ENDPOINT"),
            )
            s3 = await s3_ctx.__aenter__()
            return session, s3, s3_ctx

        session, s3, s3_ctx = self._async.submit(setup, s3_cfg).result()
        self._closed = False
        self._session = session
        self._s3 = s3
        self._s3_ctx = s3_ctx

    def close(self):
        async def _close(s3):
            await s3.close()

        if not self._closed:
            self._async.submit(_close, self._s3).result()
            self._async.terminate()
            self._closed = True

    def __del__(self):
        self.close()

    def list_dir(self, url, **kw):
        """Returns a future object"""

        async def action(url, s3, **kw):
            return await s3_dir(url, s3=s3, **kw)

        return self._async.submit(action, url, self._s3, **kw)

    def find_all(self, url, pred=None, glob=None, **kw):
        """List all objects under certain path

        Returns a future object that resolves to a list of s3 object metadata

        each s3 object is represented by a SimpleNamespace with attributes:
        - url
        - size
        - last_modified
        - etag
        """
        if glob is None and isinstance(pred, str):
            pred, glob = None, pred

        async def action(url, s3, **kw):
            return await s3_find(url, s3=s3, pred=pred, glob=glob, **kw)

        return self._async.submit(action, url, self._s3, **kw)

    def find(self, url, pred=None, glob=None, **kw):
        """List all objects under certain path

        Returns an iterator of s3 object metadata

        each s3 object is represented by a SimpleNamespace with attributes:
        - url
        - size
        - last_modified
        - etag
        """
        if glob is None and isinstance(pred, str):
            pred, glob = None, pred

        async def find_to_queue(url, s3, q, **kw):
            async def on_file(x):
                await q.put(x)

            try:
                await _s3_find_via_cbk(url, on_file, s3=s3, pred=pred, glob=glob, **kw)
            except Exception:  # pylint:disable=broad-except
                return False
            finally:
                await q.put(EOS_MARKER)
            return True

        q = asyncio.Queue(1000)
        ff = self._async.submit(find_to_queue, url, self._s3, q, **kw)
        clean_exit = False
        raise_error = False

        try:
            yield from self._async.from_queue(q)
            raise_error = not ff.result()
            clean_exit = True
        finally:
            if not clean_exit:
                ff.cancel()
            if raise_error:
                raise IOError(f"Failed to list: {url}")

    def dir_dir(self, url, depth, pred=None, **kw):
        async def action(q, s3, **kw):
            try:
                await s3_dir_dir(url, depth, q, s3, pred=pred, **kw)
            finally:
                await q.put(EOS_MARKER)

        q = asyncio.Queue(1000)
        ff = self._async.submit(action, q, self._s3, **kw)
        clean_exit = False

        try:
            yield from self._async.from_queue(q)
            ff.result()
            clean_exit = True
        finally:
            if not clean_exit:
                ff.cancel()

    def head_object(self, url, **kw):
        return self._async.submit(s3_head_object, url, s3=self._s3, **kw)

    def fetch(self, url, _range=None, **kw):
        """Returns a future object"""
        return self._async.submit(
            _s3_fetch_object, url, s3=self._s3, range=_range, **kw
        )

    def __call__(self, urls, **kw):
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

        def generate_requests(urls, s3, **kw):
            for url in urls:
                if isinstance(url, tuple):
                    url, _range = url
                else:
                    _range = None

                yield self._async.submit(
                    _s3_fetch_object, url, s3=s3, _range=_range, **kw
                )

        for rr, ee in future_results(
            generate_requests(urls, self._s3, **kw), self._nconcurrent * 2
        ):
            if ee is not None:
                assert not "s3_fetch_object should not raise exceptions, but did"
            else:
                yield rr


def s3_find_glob(
    glob_pattern: str, skip_check: bool = False, s3: Optional[S3Fetcher] = None, **kw
) -> Iterator[Any]:
    """
    Build generator from supplied S3 URI glob pattern

    Arguments:
        glob_pattern {str} -- Glob pattern to filter S3 Keys by
        skip_check {bool} -- Skip validity check for S3 Key
    Raises:
        ve: ValueError if the glob pattern cannot be parsed
    """
    if s3 is None:
        s3 = S3Fetcher()

    def do_file_query(qq, pred, dirs_pred=None):
        for d in s3.dir_dir(qq.base, qq.depth, pred=dirs_pred, **kw):
            _, _files = s3.list_dir(d, **kw).result()
            for f in _files:
                if pred(f):
                    yield f

    def do_file_query2(qq, dirs_pred=None):
        fname = qq.file

        stream = s3.dir_dir(qq.base, qq.depth, pred=dirs_pred, **kw)

        if skip_check:
            yield from (SimpleNamespace(url=d + fname) for d in stream)
            return

        stream = (s3.head_object(d + fname, **kw) for d in stream)

        for (f, _), _ in future_results(stream, 32):
            if f is not None:
                yield f

    def do_dir_query(qq, dirs_pred=None):
        return (
            SimpleNamespace(url=url)
            for url in s3.dir_dir(qq.base, qq.depth, pred=dirs_pred, **kw)
        )

    try:
        qq = parse_query(glob_pattern)
    except ValueError as ve:
        logging.error("URI glob-pattern not understood: %s", ve)
        raise ve

    glob_or_file = qq.glob or qq.file

    if qq.depth is None and glob_or_file is None:
        stream = s3.find(qq.base, **kw)
    elif qq.depth is None or qq.depth < 0:
        if qq.glob:
            stream = s3.find(qq.base, glob=qq.glob, **kw)
        elif qq.file:
            postfix = "/" + qq.file
            stream = s3.find(qq.base, pred=lambda o: o.url.endswith(postfix), **kw)
    else:
        # fixed depth query
        _, prefix = s3_url_parse(glob_pattern)
        dirs_glob = prefix.split("/")[:-1]

        def dirs_pred(f):
            n = f.count("/")
            _glob = "/".join(dirs_glob[:n]) + "/"
            return fnmatch(f, _glob)

        if qq.glob is not None:
            pred = norm_predicate(glob=qq.glob)
            stream = do_file_query(qq, pred, dirs_pred=dirs_pred)
        elif qq.file is not None:
            stream = do_file_query2(qq, dirs_pred=dirs_pred)
        else:
            stream = do_dir_query(qq, dirs_pred=dirs_pred)

    return stream
