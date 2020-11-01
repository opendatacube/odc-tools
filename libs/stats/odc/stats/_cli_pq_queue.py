import sys
import click
from ._cli_common import main, parse_all_tasks

from odc.aws.queue import get_messages, get_queue, publish_message

@main.command('run-pq-queue')
@click.option('--verbose', '-v', is_flag=True, help='Be verbose')
@click.option('--overwrite', is_flag=True, help='Do not check if output already exists')
@click.option('--location', type=str)
@click.option('--threads', type=int, help='Number of worker threads', default=0)
@click.argument('cache_file', type=str, nargs=1)
@click.argument('queue', type=str, nargs=1)

def run_pq_queue(cache_file, queue, verbose, threads, overwrite, location):
    dryrun = False
    public = False
    """
    Run Pixel Quality stats

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

    If no tasks are supplied the whole file will be processed.
    """
    from tqdm.auto import tqdm
    from functools import partial
    from .io import S3COGSink
    from ._pq import pq_input_data, pq_reduce, pq_product
    from .proc import process_tasks
    from .tasks import TaskReader
    from datacube.utils.dask import start_local_dask
    from datacube.utils.rio import configure_s3_access

    # config
    resampling = 'nearest'
    COG_OPTS = dict(compress='deflate',
                    predict=2,
                    zlevel=6,
                    blocksize=800)
    # ..

    rdr = TaskReader(cache_file)
    product = pq_product(location=location)

    if verbose:
        print(repr(rdr))

    def pq_proc(task):
        ds_in = pq_input_data(task, resampling=resampling)
        ds = pq_reduce(ds_in)
        return ds

    def start_streaming(tasks, product):
        _tasks = rdr.stream(tasks, product)
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

            results = process_tasks(_tasks, pq_proc, client, sink,
                                    check_exists=not overwrite,
                                    verbose=verbose)
        if not dryrun and verbose:
            results = tqdm(results, total=len(tasks))
        print("***************")
        for p in results:
            if verbose and not dryrun:
                print(p)

        if verbose:
            print("Exiting")

        if client is not None:
            client.close()



    queue = get_queue(queue)
    tasks = [x for x in get_messages(queue, 5)]

    if verbose:
        print(f"Read {len(tasks):,d} tasks from queue.")

    sink = S3COGSink(cog_opts=COG_OPTS,
                     public=public)

    if product.location.startswith('s3:'):
        if not sink.verify_s3_credentials():
            print("Failed to load S3 credentials")
            sys.exit(2)

    if verbose and sink._creds:
        creds_rw = sink._creds
        print(f'creds: ..{creds_rw.access_key[-5:]} ..{creds_rw.secret_key[-5:]}')

    for tsk in tasks:
        start_streaming([tsk], product)

    # _tasks = rdr.stream(tasks, product)

    # client = None
    # if not dryrun:
    #     if verbose:
    #         print("Starting local Dask cluster")

    #     client = start_local_dask(threads_per_worker=threads,
    #                               mem_safety_margin='1G')

    #     # TODO: aws_unsigned is not always desirable
    #     configure_s3_access(aws_unsigned=True,
    #                         cloud_defaults=True,
    #                         client=client)
    #     if verbose:
    #         print(client)

    #     results = process_tasks(_tasks, pq_proc, client, sink,
    #                             check_exists=not overwrite,
    #                             verbose=verbose)
    # if not dryrun and verbose:
    #     results = tqdm(results, total=len(tasks))

    # for p in results:
    #     if verbose and not dryrun:
    #         print(p)

    # if verbose:
    #     print("Exiting")

    # if client is not None:
    #     client.close()
