""" Dask Distributed Tools

   - dask_compute_stream
"""
from typing import Any, Iterable
from random import randint
import toolz
import queue
from dask.distributed import Client
import dask.bag
import threading


def _randomize(prefix):
    return "{}-{:08x}".format(prefix, randint(0, 0xFFFFFFFF))


def seq_to_bags(its: Iterable[Any], chunk_sz: int, name: str = "data"):
    """Take a stream of data items and return a stream of dask.bag.Bag
    each bag (except last) containing ``chunk_sz`` elements in 1 partition.
    """
    for chunk in toolz.partition_all(chunk_sz, its):
        prefix = _randomize(name)
        dsk = {(prefix, 0): chunk}
        yield dask.bag.Bag(dsk, prefix, 1)


def dask_compute_stream(
    client: Client,
    func: Any,
    its: Iterable[Any],
    lump: int = 10,
    max_in_flight: int = 1000,
    name: str = "compute",
) -> Iterable[Any]:
    """Parallel map with back pressure.

    Equivalent to this:

       (func(x) for x in its)

    Except that ``func(x)`` runs concurrently on dask cluster.

    :param client: Connected dask client
    :param func:   Method that will be applied concurrently to data from ``its``
    :param its:    Iterator of input values
    :param lump:   Group this many datasets into one task
    :param max_in_flight: Maximum number of active tasks to submit
    :param name:   Dask name for computation
    """

    def lump_proc(dd):
        if dd is None:
            return None
        return [func(d) for d in dd]

    max_in_flight = max(2, max_in_flight // lump)
    wrk_q = queue.Queue(maxsize=max_in_flight)

    data_name = _randomize("data_" + name)
    name = _randomize(name)
    priority = 2 ** 31

    def feeder(its, lump, q, client):
        for i, x in enumerate(toolz.partition_all(lump, its)):
            key = name + str(i)
            data_key = data_name + str(i)
            task = client.get(
                {key: (lump_proc, data_key), data_key: x},
                key,
                priority=priority - i,
                sync=False,
            )
            q.put(task)  # maybe blocking

        q.put(None)  # EOS marker

    in_thread = threading.Thread(target=feeder, args=(its, lump, wrk_q, client))
    in_thread.start()

    while True:
        yy = wrk_q.get()  # maybe blocking

        if yy is None:
            break

        yield from yy.result()
        del yy

    in_thread.join()
