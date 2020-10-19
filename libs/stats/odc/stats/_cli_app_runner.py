from pathlib import Path
import os
import click

from odc.aws.queues import get_messages, get_queue, publish_message
from ._cli_run_pq import run_pq
from ._cli_common import main, parse_all_tasks
from .tasks import TaskReader

@main.command('run-from-queue')
@click.option('--command', type=str)
@click.option('--db', type=str)
@click.option('--queue', type=str)
@click.option('--location', type=str)


def run_from_queue(command, db, queue, location):
    def get_tasks(cache_file):
        rdr = TaskReader(cache_file)
        tasks = rdr.all_tiles
        print(f"Found {len(tasks):,d} tasks in the file")
        return tasks

    queue = get_queue(queue)
    messages = get_messages(queue, 5)
    if command == 'run-pq':
        for msg in messages:
            tidx = msg.body
            cmd = f"odc-stats run-pq --location {location} {db} {tidx} --verbose"
            os.system(cmd)
    elif command == 'publish-messages':
        tasks = get_tasks(db)
        for ta in tasks:
            task = (ta[0], str(ta[1]), str(ta[2]))
            task = ','.join(task)
            publish_message(queue, task)
    else:
        raise Exception('Support for {command} has not been implemented.')
