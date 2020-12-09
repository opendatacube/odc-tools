import sys
import click
import logging

from ._cli_common import main, parse_all_tasks

from odc.aws.queue import get_messages, get_queue, publish_message


@main.command("run-pq-queue")
@click.option("--verbose", "-v", is_flag=True, help="Be verbose")
@click.option("--overwrite", is_flag=True, help="Do not check if output already exists")
@click.option("--output-location", type=str)
@click.option("--limit", type=int)
@click.option("--threads", type=int, help="Number of worker threads", default=0)
@click.argument("cache_file", type=str, nargs=1)
@click.argument("queue", type=str, nargs=1)
def run_pq_queue(
    cache_file, queue, verbose, limit, threads, overwrite, output_location
):
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
    from .proc import process_task
    from .tasks import TaskReader
    from datacube.utils.dask import start_local_dask
    from datacube.utils.rio import configure_s3_access

    # config
    resampling = "nearest"
    COG_OPTS = dict(compress="deflate", predict=2, zlevel=6, blocksize=800)
    # ..

    # TODO: Handle cache file from LOCAL or S3, maybe using
    # https://github.com/opendatacube/datacube-alchemist/blob/main/datacube_alchemist/worker.py#L44
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
    for message in get_messages(queue, limit):
        try:
            task_def = get_task_from_message(message)
            _task = rdr.load_tile(task_def, product)
            configure_s3_access(aws_unsigned=True, cloud_defaults=True, client=client)
            if verbose:
                print(client)

            result = process_task(
                _task,
                pq_proc,
                client,
                sink,
                check_exists=not overwrite,
                verbose=verbose,
            )

            if result:
                logging.info(f"{message} completed")
                message.delete()
                successes += 1
        except Exception as e:
            errors += 1
            logging.error(f"Failed to run {task} on dataset with error {e}")

    if errors > 0:
        logging.error(f"There were {errors} tasks that failed to execute.")
        sys.exit(errors)
    if limit and (errors + successes) < limit:
        logging.warning(f"There were {errors} tasks out of {limit} failed ")
    if client is not None:
        client.close()
    rdr._delete_local_cache()