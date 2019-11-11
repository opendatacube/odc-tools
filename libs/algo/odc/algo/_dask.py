""" Generic dask helpers
"""
from dask.distributed import wait as dask_wait
from toolz import partition_all


def chunked_persist(data, n_concurrent, client, verbose=False):
    """Force limited concurrency when persisting a large collection.

       This is useful to control memory usage when operating close to capacity.

       Sometimes `client.persist(data)` will run out of memory, not because
       fully-realized data is large, but because of intermediate data memory
       requirements. This is particularly common when using local dask cluster
       with only one worker.

       This function forces evaluation order of the dask graph to control peak
       memory usage.

       Say you have a largish task graph of 10x10 top-level sub-tasks, you have
       enough memory to process 5 sub-tasks concurrently, but Dask might decide
       to schedule more than that and will cause worker restarts due to out of
       memory errors. With this function you can force dask scheduler to
       persist this collection in batches of 5 concurrent sub-tasks, keeping
       the computation within the memory budget.
    """
    delayed = data.to_delayed().ravel()

    persisted = []
    for chunk in partition_all(n_concurrent, delayed):
        chunk = client.persist(chunk)
        _ = dask_wait(chunk)
        persisted.extend(chunk)
        if verbose:
            print('.', end='')

    # at this point it should be almost no-op
    return client.persist(data)
