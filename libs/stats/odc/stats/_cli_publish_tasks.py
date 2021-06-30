import sys
import click
import json

from ._cli_common import main, parse_all_tasks


def do_dry_run(tasks):
    for period, ix, iy in tasks:
        print(f"{period}/{ix:+04d}/{iy:+04d}")


@main.command("publish-tasks")
@click.argument("db", type=str)
@click.argument("queue", type=str)
@click.option("--verbose", "-v", is_flag=True, help="Be verbose")
@click.option(
    "--dryrun", is_flag=True, help="Do not publish just print what would be submitted"
)
@click.option(
    "--bunch-size", type=int, default=10, help="Number of messages to submit in one go"
)
@click.argument("tasks", type=str, nargs=-1)
def publish_to_queue(db, queue, verbose, dryrun, bunch_size, tasks):
    """
    Publish tasks to SQS.

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

    If no tasks are supplied all tasks will be published the queue.
    """
    from odc.aws.queue import get_queue, publish_messages
    from .tasks import TaskReader, render_sqs
    import toolz

    rdr = TaskReader(db)
    if len(tasks) == 0:
        tasks = rdr.all_tiles
        if verbose:
            print(f"Found {len(tasks):,d} tasks in the file")
    else:
        try:
            tasks = parse_all_tasks(tasks, rdr.all_tiles)
        except ValueError as e:
            print(str(e), file=sys.stderr)
            sys.exit(1)

    if dryrun:
        do_dry_run(tasks)
        sys.exit(0)

    queue = get_queue(queue)
    
    # We assume the db files are always be the S3 uri. If they are not, there is no need to use SQS queue to process.
    messages = (
        dict(Id=str(idx), MessageBody=json.dumps(render_sqs(tidx, db)))
        for idx, tidx in enumerate(tasks)
    )

    for bunch in toolz.partition_all(bunch_size, messages):
        publish_messages(queue, bunch)
