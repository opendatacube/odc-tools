import tarfile
import datetime
import io
import time
from pathlib import Path
import itertools


def tar_mode(gzip=None, xz=None, is_pipe=None):
    """Return tarfile.open compatible mode from boolean flags"""
    if gzip:
        return ":gz"
    if xz:
        return ":xz"
    if is_pipe:
        return "|"
    return ""


def tar_doc_stream(fname, mode=None, predicate=None):
    """Read small documents (whole doc must fit into memory) from tar file.

    predicate : entry_info -> Bool
       return True for those entries that need to be read and False for those that need to be skipped.

       where `entry_info` is a tar entry info dictionary with keys like:
         name   -- internal path
         size   -- size in bytes
         mtime  -- timestamp as and integer

    mode: passed on to tarfile.open(..), things like 'r:gz'


    Function returns iterator of tuples (name:str, data:bytes)
    """
    if predicate:

        def should_skip(entry):
            if not entry.isfile():
                return True
            return not predicate(entry.get_info())

    else:

        def should_skip(entry):
            return not entry.isfile()

    def tar_open(fname, mode):
        if isinstance(fname, (str, Path)):
            open_args = [mode] if mode is not None else []
            return tarfile.open(fname, *open_args)

        return tarfile.open(mode=mode, fileobj=fname)

    with tar_open(fname, mode) as tar:
        ee_stream = itertools.filterfalse(should_skip, tar)

        for entry in ee_stream:
            with tar.extractfile(entry) as f:
                buf = f.read()
                yield entry.name, buf


def add_txt_file(tar, fname, content, mode=0o644, last_modified=None):
    """Add file to tar from RAM (string or bytes) + name

    :param tar: tar file object opened for writing
    :param fname: path within tar file
    :param content: string or bytes, content or the file to write
    :param mode: file permissions octet
    :param last_modified: file modification timestamp
    """
    if last_modified is None:
        last_modified = time.time()

    if isinstance(last_modified, datetime.datetime):
        last_modified = last_modified.timestamp()

    info = tarfile.TarInfo(name=fname)
    if isinstance(content, str):
        content = content.encode("utf-8")
    info.size = len(content)
    info.mtime = last_modified
    info.mode = mode
    tar.addfile(tarinfo=info, fileobj=io.BytesIO(content))
