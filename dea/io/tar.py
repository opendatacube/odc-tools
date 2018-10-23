import tarfile
import itertools


def tar_doc_stream(fname, mode=None, predicate=None):
    """ Read small documents (whole doc must fit into memory) from tar file.

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

    open_args = [mode] if mode is not None else []

    with tarfile.open(fname, *open_args) as tar:
        ee_stream = itertools.filterfalse(should_skip, tar)

        for entry in ee_stream:
            with tar.extractfile(entry) as f:
                buf = f.read()
                yield entry.name, buf
