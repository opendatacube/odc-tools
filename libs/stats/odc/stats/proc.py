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

from .model import Task, TaskResult, TaskRunnerConfig
from .io import S3COGSink
from .tasks import TaskReader
from . import _plugins
from odc.algo import chunked_persist_da
from datacube.utils.dask import start_local_dask
from datacube.utils.rio import configure_s3_access


Future = Any
TaskProc = Callable[[Task], Union[xr.Dataset, xr.DataArray]]


def _drain(
    futures: Set[Future], task_cache: Dict[str, Task], timeout: Optional[float] = None,
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


class TaskRunner:
    def __init__(
        self, cfg: TaskRunnerConfig, resolution: Optional[Tuple[float, float]] = None
    ):
        """

        """
        _log = logging.getLogger(__name__)
        self._cfg = cfg
        self._log = _log
        self.sink = S3COGSink(
            cog_opts=cfg.cog_opts,
            cog_opts_per_band=cfg.cog_opts_per_band,
            public=cfg.s3_public,
        )

        _log.info(f"Resolving plugin: {cfg.plugin}")
        mk_proc = _plugins.resolve(cfg.plugin)
        self.proc = mk_proc(**cfg.plugin_config)
        self.product = self.proc.product(cfg.output_location)
        _log.info(f"Output product: {self.product}")

        _log.info(f"Constructing task reader: {cfg.filedb}")
        self.rdr = TaskReader(cfg.filedb, self.product)
        _log.info(f"Will read from {self.rdr}")
        if resolution is not None:
            _log.info(f"Changing resolution to {resolution[0], resolution[1]}")
            if self.rdr.is_compatible_resolution(resolution):
                self.rdr.change_resolution(resolution)
            else:
                _log.error(
                    f"Requested resolution is not compatible with GridSpec in '{cfg.filedb}'"
                )
                raise ValueError(
                    f"Requested resolution is not compatible with GridSpec in '{cfg.filedb}'"
                )

        self._client = None

    def _init_dask(self) -> Client:
        cfg = self._cfg
        _log = self._log
        dask_args: Dict[str, Any] = dict(
            threads_per_worker=cfg.threads, processes=False
        )

        if cfg.memory_limit != "":
            dask_args["memory_limit"] = cfg.memory_limit
        else:
            dask_args["mem_safety_margin"] = "1G"

        client = start_local_dask(**dask_args)
        aws_unsigned = self._cfg.aws_unsigned
        for c in (None, client):
            configure_s3_access(
                aws_unsigned=aws_unsigned, cloud_defaults=True, client=c
            )
        _log.info(f"Started local Dask {client}")

        return client

    def client(self) -> Client:
        if self._client is None:
            self._client = self._init_dask()
        return self._client

    def verify_setup(self) -> bool:
        _log = self._log

        if self.product.location.startswith("s3://"):
            _log.info(f"Verifying credentials for output to {self.product.location}")

            test_uri = None  # TODO: for now just check credentials are present
            if not self.sink.verify_s3_credentials(test_uri):
                _log.error("Failed to obtain S3 credentials for writing")
                return False
        return True

    def tasks(self, tasks: List[str]) -> Iterator[Task]:
        from ._cli_common import parse_all_tasks

        if len(tasks) == 0:
            tiles = self.rdr.all_tiles
        else:
            # this can throw ValueError
            tiles = parse_all_tasks(tasks, self.rdr.all_tiles)

        return self.rdr.stream(tiles)

    def dry_run(
        self, tasks: List[str], check_exists: bool = True
    ) -> Iterator[TaskResult]:
        sink = self.sink
        overwrite = self._cfg.overwrite

        # exists (None|T|F) -> str
        flag_mapping = {
            None: "",
            False: " (new)",
            True: " (recompute)" if overwrite else " (skip)",
        }

        for task in self.tasks(tasks):
            uri = sink.uri(task)
            exists = None
            if check_exists:
                exists = sink.exists(task)

            skipped = (overwrite is False) and (exists is True)
            nds = len(task.datasets)
            # TODO: take care of utc offset for day boundaries when computing ndays
            ndays = len(set(ds.center_time.date() for ds in task.datasets))
            task_id = (
                f"{task.short_time}/{task.tile_index[0]:+05d}/{task.tile_index[1]:+05d}"
            )
            flag = flag_mapping.get(exists, "")
            msg = f"{task_id} days={ndays:03} ds={nds:04} {uri}{flag}"

            yield TaskResult(task, uri, skipped=skipped, meta=msg)

    def _safe_result(self, f: Future, task: Task) -> TaskResult:
        _log = self._log
        try:
            path, ok = f.result()
            if ok:
                return TaskResult(task, path)
            else:
                error_msg = f"Failed to write: {path}"
                _log.error(error_msg)
                return TaskResult(task, path, error=error_msg)
        except Exception as e:
            _log.error(f"Error during processing of {task.location} {e}")
            return TaskResult(task, error=str(e))

    def _run(self, tasks: Iterable[Task]) -> Iterator[TaskResult]:
        cfg = self._cfg
        client = self.client()
        sink = self.sink
        proc = self.proc
        check_exists = cfg.overwrite is False
        _log = self._log

        for task in tasks:
            _log.info(f"Starting processing of {task.location}")

            if check_exists:
                path = sink.uri(task)
                _log.debug(f"Checking if can skip {path}")
                if sink.exists(task):
                    _log.info(f"Skipped task @ {path}")
                    if task.source:
                        _log.info("Notifying completion via SQS")
                        task.source.delete()

                    yield TaskResult(task, path, skipped=True)
                    continue

            _log.debug("Building Dask Graph")
            ds = proc.reduce(proc.input_data(task))

            _log.debug(f"Submitting to Dask ({task.location})")
            ds = client.persist(ds, fifo_timeout="1ms")
            cog = sink.dump(task, ds)
            cog = client.compute(cog, fifo_timeout="1ms")

            _log.debug("Waiting for completion")
            result = self._safe_result(cog, task)
            if result:
                _log.info(f"Finished processing of {result.task.location}")
                if result.task.source:
                    _log.info("Notifying completion via SQS")
                    result.task.source.delete()

            yield result

    def run(
        self, tasks: Optional[List[str]] = None, sqs: Optional[str] = None
    ) -> Iterator[TaskResult]:
        cfg = self._cfg
        _log = self._log

        if tasks is not None:
            _log.info("Starting processing from task list")
            return self._run(self.tasks(tasks))
        if sqs is not None:
            _log.info(
                f"Processing from SQS: {sqs}, timeout: {cfg.max_processing_time} seconds"
            )
            return self._run(
                self.rdr.stream_from_sqs(
                    sqs, visibility_timeout=cfg.max_processing_time
                )
            )
        raise ValueError("Must supply one of tasks= or sqs=")
