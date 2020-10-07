from typing import Iterable, Iterator, Callable, Optional, List, Set, Any, Tuple, Union
import dask.distributed
from dask.distributed import Client, wait as dask_wait
import xarray as xr

from .model import Task
from .io import S3COGSink
from odc.algo import chunked_persist_da


Future = Any
TaskProc = Callable[[Task], Union[xr.Dataset, xr.DataArray]]


def drain(futures: Set[Future],
          timeout: Optional[float] = None) -> Tuple[List[str], Set[Future]]:
    return_when = 'FIRST_COMPLETED'
    if timeout is None:
        return_when = 'ALL_COMPLETED'

    try:
        rr = dask_wait(futures, timeout=timeout, return_when=return_when)
    except dask.distributed.TimeoutError:
        return [], futures

    done: List[str] = []
    for f in rr.done:
        try:
            path, ok = f.result()
            if ok:
                done.append(path)
            else:
                print(f"Failed to write: {path}")
        except Exception as e:
            print(e)

    return done, rr.not_done


def _with_lookahead1(it: Iterable[Any]) -> Iterator[Any]:
    NOT_SET = object()
    prev = NOT_SET
    for x in it:
        if prev is not NOT_SET:
            yield prev
        prev = x
    if prev is not NOT_SET:
        yield prev


def process_tasks(tasks: Iterable[Task],
                  proc: TaskProc,
                  client: Client,
                  sink: S3COGSink,
                  check_exists: bool = True,
                  chunked_persist: int = 0,
                  verbose: bool = True) -> Iterator[str]:

    def prep_stage(tasks: Iterable[Task],
                   proc: TaskProc) -> Iterator[Tuple[Union[xr.Dataset, xr.DataArray, None], Task, str]]:
        for task in tasks:
            path = sink.uri(task)
            if check_exists:
                if sink.exists(task):
                    yield (None, task, path)
                    continue

            ds = proc(task)
            yield (ds, task, path)

    in_flight_cogs: Set[Future] = set()
    for ds, task, path in _with_lookahead1(prep_stage(tasks, proc)):
        if ds is None:
            if verbose:
                print(f"..skipping: {path} (exists already)")
            yield path
            continue

        if chunked_persist > 0:
            assert isinstance(ds, xr.DataArray)
            ds = chunked_persist_da(ds, chunked_persist, client)
        else:
            ds = client.persist(ds, fifo_timeout='1ms')

        if len(in_flight_cogs):
            done, in_flight_cogs = drain(in_flight_cogs, 1.0)
            for r in done:
                yield r

        if isinstance(ds, xr.DataArray):
            ds = ds.to_dataset(dim='band')

        cog = client.compute(sink.dump(task, ds),
                             fifo_timeout='1ms')
        rr = dask_wait(ds)
        assert len(rr.not_done) == 0
        del ds, rr
        in_flight_cogs.add(cog)

    done, _ = drain(in_flight_cogs)
    for r in done:
        yield r
