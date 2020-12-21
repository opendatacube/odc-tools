import sys
import click
import logging
from ._cli_common import main


@main.command("run-pq-queue")
@click.option("--verbose", "-v", is_flag=True, help="Be verbose")
@click.option("--overwrite", is_flag=True, help="Do not check if output already exists")
@click.option("--output-location", type=str)
@click.option('--public/--no-public', is_flag=True, default=False,
              help='Mark outputs for public access (default: no)')
@click.option("--threads", type=int, help="Number of worker threads", default=0)
@click.option('--memory-limit', type=str, help='Limit memory used by Dask cluster', default='')
@click.option('--max-processing-time', type=int, help='Max seconds per task', default=3600)
@click.argument("cache_file", type=str, nargs=1)
@click.argument("queue", type=str, nargs=1)
def run_pq_queue(
    verbose,
    overwrite,
    output_location,
    public,
    threads,
    memory_limit,
    max_processing_time,
    cache_file,
    queue,
):
    """
    Run Pixel Quality stats on tasks provided in a SQS queue

    \b
    E.g:
        odc-stats run-pq-queue s3://deafrica-stats-processing/orchestration_test/s2_l2a_2020--P1Y.db \\
              deafrica-prod-eks-stats-geomedian \\
              --output-location s3://deafrica-stats-processing/orchestration_test/output/

    """
    import psutil
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
                    zlevel=9,
                    blocksize=800,
                    ovr_blocksize=256,  # ovr_blocksize must be powers of 2 for some reason in GDAL
                    overview_resampling='average')
    ncpus = psutil.cpu_count()
    # ..

    if threads <= 0:
        threads = ncpus

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

    client = None

    if verbose:
        print("Starting local Dask cluster")

    dask_args = dict(threads_per_worker=threads,
                     processes=False)
    if memory_limit != "":
        dask_args["memory_limit"] = memory_limit
    else:
        dask_args["mem_safety_margin"] = "1G"

    client = start_local_dask(**dask_args)
    configure_s3_access(aws_unsigned=True, cloud_defaults=True, client=client)

    if verbose:
        print(client)

    tasks = rdr.stream_from_sqs(queue, product, visibility_timeout=max_processing_time)

    results = process_tasks(tasks, pq_proc, client, sink, check_exists=not overwrite, verbose=verbose)
    for task in results:
        if verbose:
            logging.info(f"Finished task: {task.location}")
        # TODO: verify we are not late
        task.source.delete()

    if client is not None:
        client.close()
