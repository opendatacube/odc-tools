import os
import json
import pytest
import boto3
from moto import mock_sns, mock_sqs
from datacube import Datacube
from click.testing import CliRunner
from odc.apps.dc_tools.s3_to_dc import cli as s3_cli
from odc.apps.dc_tools.sqs_to_dc import cli as sqs_cli
from odc.apps.dc_tools.fs_to_dc import cli as fs_cli
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
def test_s3_publishing_action_from_stac(
    aws_credentials, aws_env, odc_db_for_sns, s2am_dsid
):
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
    sns_arn = topic.get("TopicArn")
    sns.subscribe(
        TopicArn=sns_arn,
        Protocol="sqs",
        Endpoint=queue_arn,
    )

    dc = odc_db_for_sns
    assert dc.index.datasets.get(s2am_dsid) is None

    runner = CliRunner()
    # This will fail if requester pays is enabled
    result = runner.invoke(
        s3_cli,
        [
            "--no-sign-request",
            "--stac",
            "--skip-lineage",
            "--update-if-exists",
            f"--publish-action={sns_arn}",
            "s3://dea-public-data/baseline/ga_s2am_ard_3/49/JFM/2016/12/14/20161214T092514/*stac-item.json",
            "ga_s2am_ard_3",
        ],
        catch_exceptions=False,
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
    assert message_attrs["product"].get("Value") == "ga_s2am_ard_3"


@mock_sns
@mock_sqs
def test_s3_publishing_action_from_eo3(
    aws_credentials, aws_env, odc_db_for_sns, s2am_dsid
):
    """Same as above but requiring stac to eo3 conversion"""
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
    sns_arn = topic.get("TopicArn")
    sns.subscribe(
        TopicArn=sns_arn,
        Protocol="sqs",
        Endpoint=queue_arn,
    )

    dc = odc_db_for_sns
    assert dc.index.datasets.get(s2am_dsid) is None

    runner = CliRunner()
    result = runner.invoke(
        s3_cli,
        [
            "--statsd-setting",
            "localhost:8125",
            "--no-sign-request",
            "--update-if-exists",
            "--skip-lineage",
            f"--publish-action={sns_arn}",
            "s3://dea-public-data/baseline/ga_s2am_ard_3/49/JFM/2016/12/14/20161214T092514/*odc-metadata.yaml",
            "ga_s2am_ard_3",
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
def test_sqs_publishing(aws_credentials, aws_env, sqs_message, odc_db_for_sns):
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
    sns_arn = topic.get("TopicArn")
    sns.subscribe(
        TopicArn=sns_arn,
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
            f"--publish-action={sns_arn}",
        ],
    )

    assert result.exit_code == 0

    messages = sqs.receive_message(
        QueueUrl=queue.get("QueueUrl"),
    )["Messages"]
    assert len(messages) == 1
    message_attrs = json.loads(messages[0]["Body"]).get("MessageAttributes")
    assert message_attrs["action"].get("Value") == "ADDED"
    assert message_attrs["product"].get("Value") == "ga_ls5t_nbart_gm_cyear_3"
    assert message_attrs["maturity"].get("Value") == "final"


@mock_sns
@mock_sqs
def test_sqs_publishing_archive(
    aws_credentials, aws_env, sqs_message, odc_db_for_archive, ls5t_dsid
):
    """Test that archive action is published with archive flag"""
    sns = boto3.client("sns")
    sqs = boto3.client("sqs")

    input_queue_name = "input-queue"
    input_queue = sqs.create_queue(QueueName=input_queue_name)
    sqs.send_message(
        QueueUrl=input_queue.get("QueueUrl"),
        MessageBody=json.dumps(sqs_message),
    )

    topic = sns.create_topic(Name="test_topic")
    queue = sqs.create_queue(QueueName="test-queue")
    attrs = sqs.get_queue_attributes(
        QueueUrl=queue.get("QueueUrl"), AttributeNames=["All"]
    )
    queue_arn = attrs["Attributes"]["QueueArn"]
    sns_arn = topic.get("TopicArn")

    sns.subscribe(
        TopicArn=sns_arn,
        Protocol="sqs",
        Endpoint=queue_arn,
    )

    dc = odc_db_for_archive
    assert dc.index.datasets.get(ls5t_dsid) is not None

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
            f"--publish-action={sns_arn}",
        ],
    )

    assert result.exit_code == 0
    
    messages = sqs.receive_message(
        QueueUrl=queue.get("QueueUrl"),
    )["Messages"]
    assert len(messages) == 1
    message_attrs = json.loads(messages[0]["Body"]).get("MessageAttributes")
    assert message_attrs["action"].get("Value") == "ARCHIVED"


@mock_sqs
@mock_sns
def test_with_archive_less_mature(
    aws_credentials, aws_env, odc_db_for_maturity_tests, nrt_dsid, final_dsid
):
    sns = boto3.client("sns")
    sqs = boto3.client("sqs")

    topic = sns.create_topic(Name="test-topic")
    queue = sqs.create_queue(QueueName="test-queue")
    attrs = sqs.get_queue_attributes(
        QueueUrl=queue.get("QueueUrl"), AttributeNames=["All"]
    )
    queue_arn = attrs["Attributes"]["QueueArn"]
    sns_arn = topic.get("TopicArn")
    sns.subscribe(
        TopicArn=sns_arn,
        Protocol="sqs",
        Endpoint=queue_arn,
    )

    dc = odc_db_for_maturity_tests
    assert dc.index.datasets.get(nrt_dsid) is None

    runner = CliRunner()
    nrt_result = runner.invoke(
        fs_cli,
        [
            str(TEST_DATA_FOLDER),
            "--glob=**/maturity-nrt.odc-metadata.yaml",
            "--statsd-setting",
            "localhost:8125",
            "--archive-less-mature",
            f"--publish-action={sns_arn}",
        ],
    )

    assert nrt_result.exit_code == 0
    assert dc.index.datasets.get(nrt_dsid) is not None

    messages = sqs.receive_message(
        QueueUrl=queue.get("QueueUrl"),
    )["Messages"]
    assert len(messages) == 1
    message = json.loads(messages[0]["Body"])
    message_attrs = message.get("MessageAttributes")
    assert message_attrs["action"].get("Value") == "ADDED"
    assert json.loads(message["Message"]).get("id") == nrt_dsid

    assert dc.index.datasets.get(final_dsid) is None

    final_result = runner.invoke(
        fs_cli,
        [
            str(TEST_DATA_FOLDER),
            "--glob=**/maturity-final.odc-metadata.yaml",
            "--statsd-setting",
            "localhost:8125",
            "--archive-less-mature",
            f"--publish-action={sns_arn}",
        ],
    )

    assert final_result.exit_code == 0
    assert dc.index.datasets.get(final_dsid) is not None

    messages = sqs.receive_message(
        QueueUrl=queue.get("QueueUrl"),
        MaxNumberOfMessages=10,
    )["Messages"]
    assert len(messages) == 2

    nrt_message = json.loads(messages[0]["Body"])
    assert nrt_message.get("MessageAttributes")["action"].get("Value") == "ARCHIVED"
    assert json.loads(nrt_message["Message"]).get("id") == nrt_dsid

    final_message = json.loads(messages[1]["Body"])
    assert final_message.get("MessageAttributes")["action"].get("Value") == "ADDED"
    assert json.loads(final_message["Message"]).get("id") == final_dsid
