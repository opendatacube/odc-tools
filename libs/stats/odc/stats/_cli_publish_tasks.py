from pathlib import Path
import os
import click

from odc.aws.queue import get_queue, publish_message
from ._cli_run_pq import run_pq
from ._cli_common import main, parse_all_tasks
from .tasks import TaskReader


@main.command("publish-tasks")
@click.argument("db", type=str)
@click.argument("queue", type=str)
def publish_to_queue(db, queue):
    def get_tasks(cache_file):
        rdr = TaskReader(cache_file)
        print(rdr)
        tasks = rdr.all_tiles
        print(f"Found {len(tasks):,d} tasks in the file")
        return tasks

    queue = get_queue(queue)
    tasks = get_tasks(db)
    for ta in tasks:
        task = (ta[0], str(ta[1]), str(ta[2]))
        task = ",".join(task)
        # ToDo: add a check that ensures a message has been delivered
        publish_message(queue, task)
