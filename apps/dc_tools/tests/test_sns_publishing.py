import os
import json
import pytest
import boto3
from moto import mock_sns, mock_sqs
from datacube import Datacube
from click.testing import CliRunner
from odc.apps.dc_tools.s3_to_dc import cli as s3_cli
from odc.apps.dc_tools.sqs_to_dc import cli as sqs_cli
from datacube import Datacube
from pathlib import Path


@pytest.fixture
def aws_credentials():
    """Mocked AWS Credentials for moto."""
    os.environ["AWS_ACCESS_KEY_ID"] = "testing"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
    os.environ["AWS_SECURITY_TOKEN"] = "testing"
    os.environ["AWS_SESSION_TOKEN"] = "testing"


@mock_sns
@mock_sqs
def test_s3_publishing_action_from_stac(aws_credentials, aws_env):
    # set up sns topic and the sqs queue that is subscribed to it
    sns = boto3.client("sns")
    sqs = boto3.client("sqs")
    topic_name = "test-topic"
    queue_name = "test-queue"

    topic = sns.create_topic(Name=topic_name)
    queue = sqs.create_queue(QueueName=queue_name)
    attrs = sqs.get_queue_attributes(
        QueueUrl=queue.get("QueueUrl"), AttributeNames=["All"]
    )
    queue_arn = attrs["Attributes"]["QueueArn"]

    sns.subscribe(
        TopicArn=topic.get("TopicArn"),
        Protocol="sqs",
        Endpoint=queue_arn,
    )

    runner = CliRunner()
    # This will fail if requester pays is enabled
    result = runner.invoke(
        s3_cli,
        [
            "--statsd-setting",
            "localhost:8125",
            "--no-sign-request",
            "--stac",
            "--update-if-exists",
            f"--publish-action={topic_name}",
            "s3://sentinel-cogs/sentinel-s2-l2a-cogs/42/T/UM/2022/1/S2A_42TUM_20220102_0_L2A/*.json",
            "s2_l2a",
        ],
    )

    assert result.exit_code == 0
    assert (
        result.output == "Added 1 datasets, skipped 0 datasets and failed 0 datasets.\n"
    )
    messages = sqs.receive_message(
        QueueUrl=queue.get("QueueUrl"),
    )["Messages"]
    assert len(messages) == 1
    message_attrs = json.loads(messages[0]["Body"]).get("MessageAttributes")
    assert message_attrs["action"].get("Value") == "ADDED"
    assert message_attrs["product"] == "s2_l2a"


@mock_sns
@mock_sqs
def test_s3_publishing_action_from_eo3(aws_credentials, aws_env):
    """Same as above but requiring stac to eo3 conversion"""
    # set up sns topic and the sqs queue that is subscribed to it
    sns = boto3.client("sns")
    sqs = boto3.client("sqs")
    topic_name = "test-topic"
    queue_name = "test-queue"

    topic = sns.create_topic(Name=topic_name)
    queue = sqs.create_queue(QueueName=queue_name)
    attrs = sqs.get_queue_attributes(
        QueueUrl=queue.get("QueueUrl"), AttributeNames=["All"]
    )
    queue_arn = attrs["Attributes"]["QueueArn"]

    sns.subscribe(
        TopicArn=topic.get("TopicArn"),
        Protocol="sqs",
        Endpoint=queue_arn,
    )

    runner = CliRunner()
    result = runner.invoke(
        s3_cli,
        [
            "--statsd-setting",
            "localhost:8125",
            "--no-sign-request",
            "--update-if-exists",
            f"--publish-action={topic_name}",
            "s3://dea-public-data/cemp_insar/insar/displacement/alos/2010/01/07/*.yaml",
            "cemp_insar_alos_displacement",
        ],
    )

    assert result.exit_code == 0
    assert (
        result.output == "Added 1 datasets, skipped 0 datasets and failed 0 datasets.\n"
    )
    messages = sqs.receive_message(
        QueueUrl=queue.get("QueueUrl"),
    )["Messages"]
    assert len(messages) == 1
    message_attrs = json.loads(messages[0]["Body"]).get("MessageAttributes")
    assert message_attrs["action"].get("Value") == "ADDED"


TEST_DATA_FOLDER: Path = Path(__file__).parent.joinpath("data")
STAC_DATA: str = "ga_ls5t_nbart_gm_cyear_3_x30y14_1999--P1Y_final.stac-item.json"


@pytest.fixture
def sqs_message():
    with TEST_DATA_FOLDER.joinpath(STAC_DATA).open("r") as f:
        body = json.load(f)
    message = {
        "Type": "Notification",
        "MessageId": "xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxxxxxxxxx",
        "TopicArn": "arn:aws:sns:ap-southeast-2:xxxxxxxxxxxxxxxxx:test-topic",
        "Subject": "Amazon S3 Notification",
        "Message": json.dumps(body),
        "Timestamp": "2020-08-21T08:28:45.921Z",
        "SignatureVersion": "1",
        "Signature": "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx",
        "SigningCertURL": "https://sns.ap-southeast-2.amazonaws.com/SimpleNotificationService-xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx.pem",
        "UnsubscribeURL": "https://sns.ap-southeast-2.amazonaws.com/?Action=Unsubscribe&SubscriptionArn=arn:aws:sns:ap-southeast-2:xxxxxxxxxxxxxxx:test-topic:xxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxxxx",
    }
    return message


@mock_sns
@mock_sqs
def test_sqs_publishing(aws_credentials, aws_env, sqs_message):
    "Test that actions are published with sqs_to_dc"
    sns = boto3.client("sns")
    sqs = boto3.client("sqs")

    topic_name = "test-topic"
    queue_name = "test-queue"
    input_queue_name = "input-queue"

    input_queue = sqs.create_queue(QueueName=input_queue_name)
    sqs.send_message(
        QueueUrl=input_queue.get("QueueUrl"),
        MessageBody=json.dumps(sqs_message),
    )

    topic = sns.create_topic(Name=topic_name)
    queue = sqs.create_queue(QueueName=queue_name)
    attrs = sqs.get_queue_attributes(
        QueueUrl=queue.get("QueueUrl"), AttributeNames=["All"]
    )
    queue_arn = attrs["Attributes"]["QueueArn"]

    sns.subscribe(
        TopicArn=topic.get("TopicArn"),
        Protocol="sqs",
        Endpoint=queue_arn,
    )

    runner = CliRunner()
    result = runner.invoke(
        sqs_cli,
        [
            input_queue_name,
            "ga_ls5t_nbart_gm_cyear_3",
            "--skip-lineage",
            "--statsd-setting",
            "localhost:8125",
            "--no-sign-request",
            "--update-if-exists",
            "--stac",
            f"--publish-action={topic_name}",
        ],
    )

    messages = sqs.receive_message(
        QueueUrl=queue.get("QueueUrl"),
    )["Messages"]
    assert result.exit_code == 0
    assert len(messages) == 1
    message_attrs = json.loads(messages[0]["Body"]).get("MessageAttributes")
    assert message_attrs["action"].get("Value") == "ADDED"
    assert message_attrs["product"].get("Value") == "ga_ls5t_nbart_gm_cyear_3"
    assert message_attrs["maturity"].get("Value") == "final"


@mock_sns
@mock_sqs
def test_sqs_publishing_archive(aws_credentials, aws_env, sqs_message):
    """Test that archive action is published with archive flag"""
    sns = boto3.client("sns")
    sqs = boto3.client("sqs")

    topic_name = "test-topic"
    queue_name = "test-queue"
    input_queue_name = "input-queue"

    input_queue = sqs.create_queue(QueueName=input_queue_name)
    sqs.purge_queue
    sqs.send_message(
        QueueUrl=input_queue.get("QueueUrl"),
        MessageBody=json.dumps(sqs_message),
    )

    topic = sns.create_topic(Name=topic_name)
    queue = sqs.create_queue(QueueName=queue_name)
    attrs = sqs.get_queue_attributes(
        QueueUrl=queue.get("QueueUrl"), AttributeNames=["All"]
    )
    queue_arn = attrs["Attributes"]["QueueArn"]

    sns.subscribe(
        TopicArn=topic.get("TopicArn"),
        Protocol="sqs",
        Endpoint=queue_arn,
    )

    dc = Datacube()
    assert dc.index.datasets.get("57814bc4-6fdf-4fa1-84e5-865b364c4284") is not None

    runner = CliRunner()
    result = runner.invoke(
        sqs_cli,
        [
            input_queue_name,
            "ga_ls5t_nbart_gm_cyear_3",
            "--skip-lineage",
            "--statsd-setting",
            "localhost:8125",
            "--no-sign-request",
            "--update-if-exists",
            "--stac",
            "--archive",
            f"--publish-action={topic_name}",
        ],
    )

    messages = sqs.receive_message(
        QueueUrl=queue.get("QueueUrl"),
    )["Messages"]
    assert result.exit_code == 0
    assert len(messages) == 1
    message_attrs = json.loads(messages[0]["Body"]).get("MessageAttributes")
    assert message_attrs["action"].get("Value") == "ARCHIVED"
