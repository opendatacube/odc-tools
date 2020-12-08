from typing import Iterable, Iterator, Callable, Optional, List, Set, Any, Tuple, Union
import dask.distributed
from dask.distributed import Client, wait as dask_wait
import xarray as xr

from .model import Task
from .io import S3COGSink
from odc.algo import chunked_persist_da


Future = Any
TaskProc = Callable[[Task], Union[xr.Dataset, xr.DataArray]]


def drain(
    futures: Set[Future], timeout: Optional[float] = None
) -> Tuple[List[str], Set[Future]]:
    return_when = "FIRST_COMPLETED"
    if timeout is None:
        return_when = "ALL_COMPLETED"

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


def process_tasks(
    task: Task,
    proc: TaskProc,
    client: Client,
    sink: S3COGSink,
    check_exists: bool = True,
    chunked_persist: int = 0,
    verbose: bool = True,
) -> Iterator[str]:
    def prep_stage(
        task: Task, proc: TaskProc
    ) -> Iterator[Tuple[Union[xr.Dataset, xr.DataArray, None], Task, str]]:
        path = sink.uri(task)
        if check_exists:
            if sink.exists(task):
                return (None, task, path)

        ds = proc(task)
        yield (ds, task, path)

    in_flight_cogs: Set[Future] = set()

    for ds, task, path in prep_stage(task, proc):
        if ds is None:
            if verbose:
                print(f"..skipping: {path} (exists already)")
            return path

        if chunked_persist > 0:
            assert isinstance(ds, xr.DataArray)
            ds = chunked_persist_da(ds, chunked_persist, client)
        else:
            ds = client.persist(ds, fifo_timeout="1ms")

        if isinstance(ds, xr.DataArray):
            attrs = ds.attrs.copy()
            ds = ds.to_dataset(dim="band")
            for dv in ds.data_vars.values():
                dv.attrs.update(attrs)

        cog = client.compute(sink.dump(task, ds), fifo_timeout="1ms")
        rr = dask_wait(ds)

        assert len(rr.not_done) == 0
        del ds, rr
        in_flight_cogs.add(cog)
        done, _ = drain(in_flight_cogs)

        print(done[0])
        return done
