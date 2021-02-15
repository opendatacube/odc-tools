import sys
import click
from ._cli_common import main, setup_logging, click_resolution, click_yaml_cfg


@main.command("run")
@click.option("--threads", type=int, help="Number of worker threads", default=0)
@click.option(
    "--memory-limit", type=str, help="Limit memory used by Dask cluster", default=""
)
@click.option(
    "--dryrun",
    is_flag=True,
    help="Do not run computation just print what work will be done",
)
@click.option("--overwrite", is_flag=True, help="Do not check if output already exists")
@click.option(
    "--public/--no-public",
    is_flag=True,
    default=False,
    help="Mark outputs for public access (default: no)",
)
@click.option(
    "--location", type=str, help="Output location prefix as a uri: s3://bucket/path/"
)
@click.option(
    "--max-processing-time", type=int, help="Max seconds per task", default=3600
)
@click.option("--from-sqs", type=str, help="Read tasks from SQS", default="")
@click.option(
    "--plugin",
    type=str,
    help="Which stats plugin to run",
    default="pq",  # TODO: remove default when dev is finished
)
@click_yaml_cfg(
    "--plugin-config", help="Config for plugin in yaml format, file or text"
)
@click_yaml_cfg("--cog-config", help="Configure COG options")
@click.option("--resampling", type=str, help="Input resampling strategy, e.g. average")
@click_resolution("--resolution", help="Override output resolution")
@click.argument("filedb", type=str, nargs=1)
@click.argument("tasks", type=str, nargs=-1)
def run(
    filedb,
    tasks,
    from_sqs,
    plugin_config,
    cog_config,
    resampling,
    resolution,
    plugin,
    dryrun,
    threads,
    memory_limit,
    overwrite,
    public,
    location,
    max_processing_time,
):
    """
    Run Stats.

    Task could be one of the 3 things

    \b
    1. Comma-separated triplet: period,x,y or 'x[+-]<int>/y[+-]<int>/period
       2019--P1Y,+003,-004
       2019--P1Y/3/-4          `/` is also accepted
       x+003/y-004/2019--P1Y   is accepted as well
    2. A zero based index
    3. A slice following python convention <start>:<stop>[:<step]
        ::10 -- every tenth task: 0,10,20,..
       1::10 -- every tenth but skip first one 1, 11, 21 ..
        :100 -- first 100 tasks

    If no tasks are supplied and --from-sqs is not used, the whole file will be processed.
    """
    setup_logging()

    import logging
    from .model import TaskRunnerConfig
    from .proc import TaskRunner
    from ._plugins import import_all

    _log = logging.getLogger(__name__)

    if from_sqs:
        if dryrun:
            print("Can not dry run from SQS")
            sys.exit(1)
        if len(tasks):
            print("Supply either <tasks> or --from-sqs")
            sys.exit(2)

    import_all()

    _cfg = dict(
        filedb=filedb,
        plugin=plugin,
        threads=threads,
        memory_limit=memory_limit,
        output_location=location,
        s3_public=public,
        overwrite=overwrite,
        max_processing_time=max_processing_time,
    )
    if resampling is not None and len(resampling) > 0:
        if plugin_config is None:
            plugin_config = {}
        plugin_config["resampling"] = resampling

    if plugin_config is not None:
        _cfg["plugin_config"] = plugin_config

    if cog_config is not None:
        _cfg["cog_opts"] = cog_config

    cfg = TaskRunnerConfig(**_cfg)
    _log.info(f"Using this config: {cfg}")

    runner = TaskRunner(cfg, resolution=resolution)
    if dryrun:
        check_exists = runner.verify_setup()
        for task in runner.dry_run(tasks, check_exists=check_exists):
            print(task.meta)
        sys.exit(0)

    if not runner.verify_setup():
        print("Failed to verify setup, exiting")
        sys.exit(1)

    result_stream = runner.run(sqs=from_sqs) if from_sqs else runner.run(tasks=tasks)

    total = 0
    finished = 0
    skipped = 0
    errored = 0
    for result in result_stream:
        total += 1
        task = result.task
        if result:
            if result.skipped:
                skipped += 1
                _log.info(f"Skipped task #{total:,d}: {task.location} {task.uuid}")
            else:
                finished += 1
                _log.info(f"Finished task #{total:,d}: {task.location} {task.uuid}")
        else:
            errored += 1
            _log.error(f"Failed task #{total:,d}: {task.location} {task.uuid}")

        _log.info(f"T:{total:,d}, OK:{finished:,d}, S:{skipped:,d}, E:{errored:,d}")

    _log.info(
        f"Completed processing {total:,d} tasks, OK:{finished:,d}, S:{skipped:,d}, E:{errored:,d}"
    )

    _log.info("Shutting down Dask cluster")
    del runner
    _log.info("Calling sys.exit(0)")
    sys.exit(0)
