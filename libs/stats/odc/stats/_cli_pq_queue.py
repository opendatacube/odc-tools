import sys
import click
import logging

from ._cli_common import main

from odc.aws.queue import get_messages, get_queue


@main.command("run-pq-queue")
@click.option("--verbose", "-v", is_flag=True, help="Be verbose")
@click.option("--overwrite", is_flag=True, help="Do not check if output already exists")
@click.option("--output-location", type=str)
@click.option('--public/--no-public', is_flag=True, default=False,
              help='Mark outputs for public access (default: no)')
@click.option("--threads", type=int, help="Number of worker threads", default=0)
@click.argument("cache_file", type=str, nargs=1)
@click.argument("queue", type=str, nargs=1)
def run_pq_queue(
        cache_file, queue, verbose, threads, overwrite, public, output_location
):
    """
    Run Pixel Quality stats on tasks provided in a SQS queue

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

    E.g:
        odc-stats run-pq-queue s3://deafrica-stats-processing/orchestration_test/s2_l2a_2020--P1Y.db\
              deafrica-prod-eks-stats-geomedian \
              --output-location s3://deafrica-stats-processing/orchestration_test/output/
    """

    from .io import S3COGSink
    from ._pq import pq_input_data, pq_reduce, pq_product
    from .proc import process_tasks
    from .tasks import TaskReader
    from datacube.utils.dask import start_local_dask
    from datacube.utils.rio import configure_s3_access

    # config
    resampling = "nearest"
    COG_OPTS = dict(compress="deflate",
                    predict=2,
                    zlevel=6,
                    ovr_blocksize=256,  # ovr_blocksize must be powers of 2 for some reason in GDAL
                    blocksize=800)
    # ..

    rdr = TaskReader(cache_file)
    product = pq_product(location=output_location)

    if verbose:
        print(repr(rdr))

    def get_task_from_message(message):
        if "," in message.body:
            msg = message.body.split(",")
            message = (msg[0], int(msg[1]), int(msg[2]))
            return message

    def pq_proc(task):
        ds_in = pq_input_data(task, resampling=resampling)
        ds = pq_reduce(ds_in)
        return ds

    sink = S3COGSink(cog_opts=COG_OPTS, public=public)
    if product.location.startswith("s3:"):
        if not sink.verify_s3_credentials():
            print("Failed to load S3 credentials")
            sys.exit(2)

    if verbose and sink._creds:
        creds_rw = sink._creds
        print(f"creds: ..{creds_rw.access_key[-5:]} ..{creds_rw.secret_key[-5:]}")

    queue = get_queue(queue)

    successes = 0
    errors = 0

    client = None

    if verbose:
        print("Starting local Dask cluster")

    client = start_local_dask(threads_per_worker=threads, mem_safety_margin="1G")
    configure_s3_access(aws_unsigned=True, cloud_defaults=True, client=client)

    if verbose:
        print(client)

    for message in get_messages(queue):
        try:
            task_def = get_task_from_message(message)
            _task = rdr.stream([task_def], product)
            results = process_tasks(
                _task,
                pq_proc,
                client,
                sink,
                check_exists=not overwrite,
                verbose=verbose,
            )
            for p in results:
                logging.info(f"{message} completed")
                message.delete()
                successes += 1
        except Exception as e:
            errors += 1
            logging.error(f"Failed to run {_task} on dataset with error {e}")

    if errors > 0:
        logging.error(f"There were {errors} tasks that failed to execute.")
        sys.exit(errors)

    if client is not None:
        client.close()
