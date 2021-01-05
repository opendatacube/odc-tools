import logging
from typing import (
    Iterable,
    Iterator,
    Callable,
    Optional,
    List,
    Set,
    Any,
    Tuple,
    Union,
    Dict,
)
import dask
import dask.distributed
from dask.distributed import Client, wait as dask_wait
import xarray as xr

from .model import Task, TaskResult
from .io import S3COGSink
from odc.algo import chunked_persist_da


Future = Any
TaskProc = Callable[[Task], Union[xr.Dataset, xr.DataArray]]


def _drain(
    futures: Set[Future],
    task_cache: Dict[str, Task],
    timeout: Optional[float] = None,
) -> Tuple[List[TaskResult], Set[Future]]:
    return_when = "FIRST_COMPLETED"
    if timeout is None:
        return_when = "ALL_COMPLETED"

    try:
        rr = dask_wait(futures, timeout=timeout, return_when=return_when)
    except dask.distributed.TimeoutError:
        return [], futures

    def safe_extract(f, task_cache) -> TaskResult:
        assert f.key in task_cache

        task = task_cache.pop(f.key)
        try:
            path, ok = f.result()
            if ok:
                return TaskResult(task, path)
            else:
                logging.error(f"Failed to write: {path}")
                return TaskResult(task, path, error="Failed to write: {path}")
        except Exception as e:
            logging.error(f"Error during processing of {task.location}: {e}")
            return TaskResult(task, error=str(e))

    done = [safe_extract(f, task_cache) for f in rr.done]
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
) -> Iterator[TaskResult]:
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

    # future.key -> Task that generated that future
    _task_cache: Dict[str, Task] = {}
    # Future: Tuple[str, bool],  (path_of_yaml, ok))
    _in_flight: Set[Future] = set()

    for ds, task, path in _with_lookahead1(prep_stage(tasks, proc)):
        if ds is None:
            if verbose:
                print(f"..skipping: {path} (exists already)")
            yield TaskResult(task, path, skipped=True)
            continue

        if chunked_persist > 0:
            assert isinstance(ds, xr.DataArray)
            ds = chunked_persist_da(ds, chunked_persist, client)
        else:
            ds = client.persist(ds, fifo_timeout="1ms")

        if len(_in_flight):
            done, _in_flight = _drain(_in_flight, _task_cache, 1.0)
            for task_result in done:
                yield task_result

        if isinstance(ds, xr.DataArray):
            attrs = ds.attrs.copy()
            ds = ds.to_dataset(dim="band")
            for dv in ds.data_vars.values():
                dv.attrs.update(attrs)

        cog = client.compute(sink.dump(task, ds), fifo_timeout="1ms")
        _task_cache[cog.key] = task

        rr = dask_wait(ds)
        assert len(rr.not_done) == 0
        del ds, rr
        _in_flight.add(cog)

    done, _ = _drain(_in_flight, _task_cache)
    for task_result in done:
        yield task_result
