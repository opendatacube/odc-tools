
from odc.aws.queues import get_messages, get_queue

from ._cli_run_pq import run_pq
from pathlib import Path
import subprocess
import sys
import click
from ._cli_common import main

def get_messages(queue, limit):
    count = 0
    while True:
        messages = queue.receive_messages(
            VisibilityTimeout=60,
            MaxNumberOfMessages=1,
            WaitTimeSeconds=10,
            MessageAttributeNames=["All"],
        )

        if len(messages) == 0 or (limit and count >= limit):
            break
        else:
            for message in messages:
                count += 1
                yield message.body

def process_queue(queue):
    for msg in get_messages(queue, 2):
        print(msg)

@main.command('run_command')
@click.option('--command', type=str)
@click.option('--db', type=str)
@click.option('--queue', type=str)
# @click.option('location', type=str)

def run_command(command, db, queue):
    import sys
    queue = get_queue(queue)
    messages = get_messages(queue, 1)

    counter = 0
    path = 's3://africa-migration-test/tmp'
    for msg in messages:
        # cmd = f"odc-stats {command} --location {path} {db} {msg}"
        if command == 'run_pq' and counter < 5:
            # subprocess.run(cmd, shell=True)
            run_pq(db, msg, True, True, 0, True, False, path)
            counter += 1
        else:
            raise Exception('Support for {command} has not been implemented.')