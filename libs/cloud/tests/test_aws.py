import json
import os
from types import SimpleNamespace

import boto3
import pytest
from moto import mock_sqs
from click.testing import CliRunner

from odc.apps.cloud import redrive_to_queue
from odc.aws.queue import redrive_queue, get_queues, get_queue
from odc.aws._find import parse_query

ALIVE_QUEUE_NAME = "mock-alive-queue"
DEAD_QUEUE_NAME = "mock-dead-queue"


def get_n_messages(queue):
    return int(queue.attributes.get("ApproximateNumberOfMessages"))


@pytest.fixture
def aws_env(monkeypatch):
    if "AWS_DEFAULT_REGION" not in os.environ:
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

    redrive_queue(DEAD_QUEUE_NAME, ALIVE_QUEUE_NAME, max_wait=0)
    assert get_n_messages(dead_queue) == 1
    assert get_n_messages(alive_queue) == 2

    # Test lots of messages:
    for i in range(35):
        dead_queue.send_message(MessageBody=json.dumps({"content": f"Something {i}"}))

    count = redrive_queue(DEAD_QUEUE_NAME, ALIVE_QUEUE_NAME, max_wait=0)
    assert count == 35

    assert get_n_messages(dead_queue) == 0


@mock_sqs
def test_redrive_to_queue_cli(aws_env):
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

    for i in range(35):
        dead_queue.send_message(MessageBody=json.dumps({"content": f"Something {i}"}))

    # Invalid value string
    returned = CliRunner().invoke(
        redrive_to_queue.cli,
        [str(DEAD_QUEUE_NAME), str(ALIVE_QUEUE_NAME), "--limit", "string_test"],
    )

    assert returned.exit_code == 1

    # Invalid value 0
    returned = CliRunner().invoke(
        redrive_to_queue.cli,
        [str(DEAD_QUEUE_NAME), str(ALIVE_QUEUE_NAME), "--limit", 0],
    )

    assert returned.exit_code == 1

    # Valid value 1
    returned = CliRunner().invoke(
        redrive_to_queue.cli,
        [str(DEAD_QUEUE_NAME), str(ALIVE_QUEUE_NAME), "--limit", 1],
    )

    assert returned.exit_code == 0
    assert int(get_queue(ALIVE_QUEUE_NAME).attributes.get('ApproximateNumberOfMessages')) == 1

    # Valid value None (all)
    returned = CliRunner().invoke(
        redrive_to_queue.cli,
        [str(DEAD_QUEUE_NAME), str(ALIVE_QUEUE_NAME), "--limit", None],
    )

    assert returned.exit_code == 0
    assert int(get_queue(DEAD_QUEUE_NAME).attributes.get('ApproximateNumberOfMessages')) == 0


@mock_sqs
def test_get_queues(aws_env):
    resource = boto3.resource("sqs")

    resource.create_queue(QueueName="a_queue1")
    resource.create_queue(QueueName="b_queue2")
    resource.create_queue(QueueName="c_queue3")
    resource.create_queue(QueueName="d_queue4")

    queues = get_queues()

    assert len(list(queues)) == 4

    # Test prefix
    queues = get_queues(prefix="a_queue1")
    assert "queue1" in list(queues)[0].url

    # Test prefix
    queues = get_queues(contains="2")
    assert "b_queue2" in list(queues)[0].url

    # Test prefix and contains
    queues = get_queues(prefix="c", contains="3")
    assert "c_queue3" in list(queues)[0].url

    # Test prefix and not contains
    queues = get_queues(prefix="d", contains="5")
    assert len(list(queues)) == 0

    # Test contains and not prefix
    queues = get_queues(prefix="q", contains="2")
    assert len(list(queues)) == 0

    # Test not found prefix
    queues = get_queues(prefix="fake_start")
    assert len(list(queues)) == 0

    # Test not found contains
    queues = get_queues(contains="not_there")
    assert len(list(queues)) == 0


@mock_sqs
def test_get_queues_empty(aws_env):
    queues = get_queues()

    assert list(queues) == []


def test_parse_query():
    E = SimpleNamespace
    base = "s3://bucket/path/a/"

    assert parse_query(base) == E(base=base, depth=None, glob=None, file=None)
    assert parse_query(base + "some") == E(
        base=base + "some/", depth=None, glob=None, file=None
    )
    assert parse_query(base + "*") == E(base=base, depth=0, glob="*", file=None)
    assert parse_query(base + "*/*txt") == E(base=base, depth=1, glob="*txt", file=None)
    assert parse_query(base + "*/*/*txt") == E(
        base=base, depth=2, glob="*txt", file=None
    )
    assert parse_query(base + "*/*/file.txt") == E(
        base=base, depth=2, glob=None, file="file.txt"
    )
    assert parse_query(base + "**/*txt") == E(
        base=base, depth=-1, glob="*txt", file=None
    )
    assert parse_query(base + "*/*/something/*yaml") == E(
        base=base, depth=3, glob="*yaml", file=None
    )

    with pytest.raises(ValueError):
        parse_query(base + "**/*/something/*yaml")
