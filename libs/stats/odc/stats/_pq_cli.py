import sys
import click
from ._cli_common import main, parse_task


@main.command('run-pq')
@click.option('--verbose', '-v', is_flag=True, help='Be verbose')
@click.option('--threads', type=int, help='Number of worker threads', default=0)
@click.option('--dryrun', is_flag=True, help='Do not run computation just print what work will be done')
@click.option('--overwrite', is_flag=True, help='Do not check if output already exists')
@click.argument('cache_file', type=str, nargs=1)
@click.argument('tasks', type=str, nargs=-1)
def run_pq(cache_file, tasks, dryrun, verbose, threads, overwrite):
    """
    Run Pixel Quality stats

    \b
    Each task is a comma-separated triplet: period,x,y
       2019--P1Y,+003,-004
       2019--P1Y/3/-4          `/` is also accepted
       x+003/y-004/2019--P1Y   is accepted as well
    """
    from tqdm.auto import tqdm
    from functools import partial
    from odc.dscache import open_ro
    from .io import S3COGSink
    from ._pq import pq_input_data, pq_reduce
    from .metadata import load_task, clear_pixel_count_product
    from .proc import process_tasks
    from datacube.utils.dask import start_local_dask
    from datacube.utils.rio import configure_s3_access

    # config
    resampling = 'nearest'
    COG_OPTS = dict(compress='deflate',
                    predict=2,
                    zlevel=6,
                    blocksize=512)
    # ..

    cache = open_ro(cache_file)
    cfg = cache.get_info_dict('stats/config')
    if verbose:
        print(cfg)

    grid = cfg['grid']
    gs = cache.grids[grid]
    pq_product = clear_pixel_count_product(gs)

    def get_task(tidx):
        return load_task(cache, tidx, pq_product, grid)

    def pq_proc(task):
        ds_in = pq_input_data(task, resampling=resampling)
        ds = pq_reduce(ds_in)
        return ds

    def dry_run_proc(task, sink, check_s3=False):
        uri = sink.uri(task)
        exists = None
        if check_s3:
            exists = sink.exists(task)

        nds = len(task.datasets)
        ndays = len(set(ds.center_time.date() for ds in task.datasets))

        if overwrite:
            flag = {None: '',
                    True: ' (recompute)',
                    False: ' (new)'}[exists]
        else:
            flag = {None: '',
                    True: ' (skip)',
                    False: ' (new)'}[exists]

        task_id = f"{task.short_time}/{task.tile_index[0]:+05d}/{task.tile_index[1]:+05d}"
        print(f"{task_id} days={ndays:03} ds={nds:04} {uri}{flag}")

        return uri

    all_tasks = sorted(idx for idx, _ in cache.tiles(grid))

    if len(tasks) == 0:
        tasks = all_tasks
        if verbose:
            print(f"Found {len(tasks):,d} tasks in the file")
    else:
        try:
            tasks = [parse_task(t) for t in tasks]
        except ValueError as e:
            print(str(e), file=sys.stderr)
            sys.exit(1)

        all_tasks_set = set(all_tasks)
        for t in tasks:
            if t not in all_tasks_set:
                print(f"No such task: {t}")
                sys.exit(2)
        del all_tasks_set

    sink = S3COGSink(cog_opts=COG_OPTS)
    if verbose:
        creds_rw = sink._creds
        print(f'creds: ..{creds_rw.access_key[-5:]} ..{creds_rw.secret_key[-5:]}')

    _tasks = map(get_task, tasks)

    client = None
    if not dryrun:
        if verbose:
            print("Starting local Dask cluster")

        client = start_local_dask(threads_per_worker=threads,
                                  mem_safety_margin='1G')

        # TODO: aws_unsigned is not always desirable
        configure_s3_access(aws_unsigned=True,
                            cloud_defaults=True,
                            client=client)
        if verbose:
            print(client)

    if dryrun:
        results = map(partial(dry_run_proc, sink=sink, check_s3=not overwrite),
                      _tasks)
    else:
        results = process_tasks(_tasks, pq_proc, client, sink,
                                check_exists=not overwrite,
                                verbose=verbose)
    if not dryrun and verbose:
        results = tqdm(results, total=len(tasks))

    for p in results:
        if verbose and not dryrun:
            print(p)

    if verbose:
        print("Exiting")

    if client is not None:
        client.close()
