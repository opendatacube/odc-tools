from typing import Iterable, Iterator, Callable, Optional, List, Set, Any, Tuple, Union, Dict
import dask
import dask.distributed
from dask.distributed import Client, wait as dask_wait
import xarray as xr

from .model import Task
from .io import S3COGSink
from odc.algo import chunked_persist_da


Future = Any
TaskProc = Callable[[Task], Union[xr.Dataset, xr.DataArray]]


def _drain(
    futures: Set[Future],
    task_cache: Dict[int, Task],
    timeout: Optional[float] = None,
) -> Tuple[List[Task], Set[Future]]:
    return_when = "FIRST_COMPLETED"
    if timeout is None:
        return_when = "ALL_COMPLETED"

    try:
        rr = dask_wait(futures, timeout=timeout, return_when=return_when)
    except dask.distributed.TimeoutError:
        return [], futures

    done: List[Task] = []
    for f in rr.done:
        try:
            task_key, (path, ok) = f.result()
            if ok:
                task = task_cache.pop(task_key)
                done.append(task)
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


def process_tasks(
    tasks: Iterable[Task],
    proc: TaskProc,
    client: Client,
    sink: S3COGSink,
    check_exists: bool = True,
    chunked_persist: int = 0,
    verbose: bool = True,
) -> Iterator[Task]:
    def prep_stage(
        tasks: Iterable[Task], proc: TaskProc
    ) -> Iterator[Tuple[Union[xr.Dataset, xr.DataArray, None], Task, str]]:
        for task in tasks:
            path = sink.uri(task)
            if check_exists:
                if sink.exists(task):
                    yield (None, task, path)
                    continue

            ds = proc(task)
            yield (ds, task, path)

    _task_cache: Dict[int, Task] = {}
    # Future: Tuple[int, Tuple[str, bool]]
    # (task_key, (path_of_yaml, ok))
    _in_flight: Set[Future] = set()

    dask_tuple = dask.delayed(lambda *x: tuple(x),
                              pure=True)

    for ds, task, path in _with_lookahead1(prep_stage(tasks, proc)):
        if ds is None:
            if verbose:
                print(f"..skipping: {path} (exists already)")
            yield task
            continue

        task_key = id(task)
        _task_cache[task_key] = task

        if chunked_persist > 0:
            assert isinstance(ds, xr.DataArray)
            ds = chunked_persist_da(ds, chunked_persist, client)
        else:
            ds = client.persist(ds, fifo_timeout="1ms")

        if len(_in_flight):
            done, _in_flight = _drain(_in_flight, _task_cache, 1.0)
            for task in done:
                yield task

        if isinstance(ds, xr.DataArray):
            attrs = ds.attrs.copy()
            ds = ds.to_dataset(dim="band")
            for dv in ds.data_vars.values():
                dv.attrs.update(attrs)

        cog = client.compute(dask_tuple(task_key, sink.dump(task, ds)),
                             fifo_timeout="1ms")
        rr = dask_wait(ds)
        assert len(rr.not_done) == 0
        del ds, rr
        _in_flight.add(cog)

    done, _ = _drain(_in_flight, _task_cache)
    for task in done:
        yield task
