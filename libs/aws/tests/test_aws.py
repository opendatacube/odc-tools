import json
import os

import boto3
from moto import mock_sqs
import pytest
from odc.aws.queue import redrive_queue

ALIVE_QUEUE_NAME = "mock-alive-queue"
DEAD_QUEUE_NAME = "mock-dead-queue"


def get_n_messages(queue):
    return int(queue.attributes.get("ApproximateNumberOfMessages"))


@pytest.fixture
def aws_env(monkeypatch):
    if 'AWS_DEFAULT_REGION' not in os.environ:
        monkeypatch.setenv("AWS_DEFAULT_REGION", "us-west-2")


@mock_sqs
def test_redrive_to_queue(aws_env):
    resource = boto3.resource("sqs")

    dead_queue = resource.create_queue(QueueName=DEAD_QUEUE_NAME)
    alive_queue = resource.create_queue(
        QueueName=ALIVE_QUEUE_NAME,
        Attributes={
            "RedrivePolicy": json.dumps(
                {
                    "deadLetterTargetArn": dead_queue.attributes.get("QueueArn"),
                    "maxReceiveCount": 2,
                }
            ),
        },
    )

    # Test redriving to a queue without an alive queue specified
    dead_queue.send_message(MessageBody=json.dumps({"test": 1}))
    assert get_n_messages(dead_queue) == 1

    count = redrive_queue(DEAD_QUEUE_NAME, max_wait=0)
    assert count == 1

    # Test redriving to a queue that is specified
    dead_queue.send_message(MessageBody=json.dumps({"test": 2}))
    assert get_n_messages(dead_queue) == 1

    count = redrive_queue(DEAD_QUEUE_NAME, ALIVE_QUEUE_NAME, max_wait=0)
    assert get_n_messages(dead_queue) == 1
    assert get_n_messages(alive_queue) == 2

    # Test lots of messages:
    for i in range(35):
        dead_queue.send_message(MessageBody=json.dumps({"content": f"Something {i}"}))

    count = redrive_queue(DEAD_QUEUE_NAME, ALIVE_QUEUE_NAME, max_wait=0)
    assert count == 35

    assert get_n_messages(dead_queue) == 0
