import boto3
import json
import os
import pytest
from click.testing import CliRunner
from moto import mock_sns, mock_sqs
from pathlib import Path

from odc.apps.dc_tools.fs_to_dc import cli as fs_cli
from odc.apps.dc_tools.s3_to_dc import cli as s3_cli
from odc.apps.dc_tools.sqs_to_dc import cli as sqs_cli


@pytest.fixture
def aws_credentials():
    """Mocked AWS Credentials for moto."""
    os.environ["AWS_ACCESS_KEY_ID"] = "testing"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
    os.environ["AWS_SECURITY_TOKEN"] = "testing"
    os.environ["AWS_SESSION_TOKEN"] = "testing"


@pytest.fixture()
def sns_setup(aws_credentials, aws_env):
    """Set up SNS topic and SQS queue subscribed to it"""
    with mock_sqs(), mock_sns():
        sns = boto3.client("sns")
        topic = sns.create_topic(Name="test-topic")
        sns_arn = topic.get("TopicArn")

        sqs = boto3.client("sqs")
        queue_name = "test-queue"
        queue = sqs.create_queue(QueueName=queue_name)
        attrs = sqs.get_queue_attributes(
            QueueUrl=queue.get("QueueUrl"), AttributeNames=["All"]
        )
        queue_arn = attrs["Attributes"]["QueueArn"]

        sns.subscribe(
            TopicArn=sns_arn,
            Protocol="sqs",
            Endpoint=queue_arn,
            Attributes={
                "RawMessageDelivery": "true",
            },
        )

        yield sns_arn, sqs, queue.get("QueueUrl")


def test_s3_publishing_action_from_stac(
    mocked_s3_datasets, odc_test_db_with_products, s2am_dsid, sns_setup
):
    sns_arn, sqs, queue_url = sns_setup

    dc = odc_test_db_with_products
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
            "s3://odc-tools-test/baseline/ga_s2am_ard_3/49/JFM/2016/12/14/20161214T092514/*stac-item.json",
            "ga_s2am_ard_3",
        ],
        catch_exceptions=False,
    )

    print(f"s3-to-dc exit_code: {result.exit_code}, output:{result.output}")
    assert result.exit_code == 0
    assert (
        result.output == "Added 1 datasets, skipped 0 datasets and failed 0 datasets.\n"
    )
    messages = sqs.receive_message(
        QueueUrl=queue_url,
        MessageAttributeNames=["All"],
    )["Messages"]
    assert len(messages) == 1
    message_attrs = messages[0].get("MessageAttributes")
    assert message_attrs["action"].get("StringValue") == "ADDED"
    assert message_attrs["product"].get("StringValue") == "ga_s2am_ard_3"


def test_s3_publishing_action_from_eo3(
    mocked_s3_datasets, odc_test_db_with_products, s2am_dsid, sns_setup
):
    """Same as above but requiring stac to eo3 conversion"""
    sns_arn, sqs, queue_url = sns_setup

    dc = odc_test_db_with_products
    assert dc.index.datasets.get(s2am_dsid) is None

    runner = CliRunner()
    result = runner.invoke(
        s3_cli,
        [
            "--no-sign-request",
            "--update-if-exists",
            "--skip-lineage",
            f"--publish-action={sns_arn}",
            "s3://odc-tools-test/baseline/ga_s2am_ard_3/49/JFM/2016/12/14/20161214T092514/*odc-metadata.yaml",
            "ga_s2am_ard_3",
        ],
        catch_exceptions=False,
    )

    print(f"s3-to-dc exit_code: {result.exit_code}, output:{result.output}")
    assert result.exit_code == 0
    assert (
        result.output == "Added 1 datasets, skipped 0 datasets and failed 0 datasets.\n"
    )

    messages = sqs.receive_message(
        QueueUrl=queue_url,
        MessageAttributeNames=["All"],
    )["Messages"]
    assert len(messages) == 1
    message_attrs = messages[0].get("MessageAttributes")
    assert message_attrs["action"].get("StringValue") == "ADDED"


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
        "Message": json.dumps(body),
        "Timestamp": "2020-08-21T08:28:45.921Z",
        "SignatureVersion": "1",
        "Signature": "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"
        "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"
        "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx",
        "SigningCertURL": "https://sns.ap-southeast-2.amazonaws.com/SimpleNotifi"
        "cationService-xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"
        ".pem",
        "UnsubscribeURL": "https://sns.ap-southeast-2.amazonaws.com/?Action=Unsubscribe"
        "&SubscriptionArn=arn:aws:sns:ap-southeast-2:xxxxxxxxxxxxxxx"
        ":test-topic:xxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxxxx",
    }
    return message


def test_sqs_publishing(
    aws_credentials, aws_env, sqs_message, odc_test_db_with_products, sns_setup
):
    """Test that actions are published with sqs_to_dc"""
    sns_arn, sqs, queue_url = sns_setup
    input_queue_name = "input-queue"

    input_queue = sqs.create_queue(QueueName=input_queue_name)
    sqs.send_message(
        QueueUrl=input_queue.get("QueueUrl"),
        MessageBody=json.dumps(sqs_message),
    )

    runner = CliRunner()
    result = runner.invoke(
        sqs_cli,
        [
            input_queue_name,
            "ga_ls5t_nbart_gm_cyear_3",
            "--skip-lineage",
            "--no-sign-request",
            "--update-if-exists",
            "--stac",
            f"--publish-action={sns_arn}",
        ],
        catch_exceptions=False,
    )
    print(f"sqs-to-dc exit_code: {result.exit_code}, output:{result.output}")

    assert result.exit_code == 0

    messages = sqs.receive_message(
        QueueUrl=queue_url,
        MessageAttributeNames=["All"],
    )["Messages"]
    assert len(messages) == 1
    message_attrs = messages[0].get("MessageAttributes")
    assert message_attrs["action"].get("StringValue") == "ADDED"
    assert message_attrs["product"].get("StringValue") == "ga_ls5t_nbart_gm_cyear_3"
    assert message_attrs["maturity"].get("StringValue") == "final"


def test_sqs_publishing_archive_flag(
    aws_credentials, aws_env, sqs_message, odc_db_for_archive, ls5t_dsid, sns_setup
):
    """Test that an ARCHIVE SNS message is published when the --archive flag is used."""
    sns_arn, sqs, queue_url = sns_setup

    input_queue_name = "input-queue"
    input_queue = sqs.create_queue(QueueName=input_queue_name)
    sqs.send_message(
        QueueUrl=input_queue.get("QueueUrl"),
        MessageBody=json.dumps(sqs_message),
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
            "--no-sign-request",
            "--update-if-exists",
            "--stac",
            "--archive",
            f"--publish-action={sns_arn}",
        ],
        catch_exceptions=False,
    )
    print(f"sqs-to-dc exit_code: {result.exit_code}, output:{result.output}")

    assert result.exit_code == 0

    messages = sqs.receive_message(
        QueueUrl=queue_url,
        MessageAttributeNames=["All"],
    )["Messages"]
    assert len(messages) == 1
    message_attrs = messages[0].get("MessageAttributes")
    assert message_attrs["action"].get("StringValue") == "ARCHIVED"


def test_sqs_publishing_archive_attribute(
    aws_credentials, aws_env, sqs_message, odc_db_for_archive, ls5t_dsid, sns_setup
):
    """Test that archiving occurs when ARCHIVED is in the message attributes"""
    sns_arn, sqs, queue_url = sns_setup

    input_queue_name = "input-queue"
    input_queue = sqs.create_queue(QueueName=input_queue_name)
    sqs.send_message(
        QueueUrl=input_queue.get("QueueUrl"),
        MessageBody=json.dumps(sqs_message),
        MessageAttributes={
            'action': {
                'DataType': 'String',
                'StringValue': 'ARCHIVED'
            }
        }
    )

    dc = odc_db_for_archive
    ds = dc.index.datasets.get(ls5t_dsid)
    assert ds is not None
    assert ds.is_archived is False

    runner = CliRunner()
    result = runner.invoke(
        sqs_cli,
        [
            input_queue_name,
            "ga_ls5t_nbart_gm_cyear_3",
            "--skip-lineage",
            "--no-sign-request",
            "--update-if-exists",
            "--stac",
            f"--publish-action={sns_arn}",
        ],
        catch_exceptions=False,
    )

    assert result.exit_code == 0
    messages = sqs.receive_message(
        QueueUrl=queue_url,
        MessageAttributeNames=["All"],
    )["Messages"]
    assert len(messages) == 1
    message_attrs = messages[0].get("MessageAttributes")
    assert message_attrs["action"].get("StringValue") == "ARCHIVED"
    assert dc.index.datasets.get(ls5t_dsid).is_archived is True


def test_with_archive_less_mature(
    aws_credentials,
    aws_env,
    odc_db,
    nrt_dsid,
    final_dsid,
    sns_setup,
):
    sns_arn, sqs, queue_url = sns_setup

    dc = odc_db
    assert dc.index.datasets.get(nrt_dsid) is None

    runner = CliRunner()
    nrt_result = runner.invoke(
        fs_cli,
        [
            str(TEST_DATA_FOLDER),
            "--glob=**/maturity-nrt.odc-metadata.yaml",
            "--archive-less-mature",
            f"--publish-action={sns_arn}",
        ],
        catch_exceptions=False,
    )
    print(f"fs-to-dc exit_code: {nrt_result.exit_code}, " "output:{nrt_result.output}")

    assert nrt_result.exit_code == 0
    assert dc.index.datasets.get(nrt_dsid) is not None

    messages = sqs.receive_message(
        QueueUrl=queue_url,
        MessageAttributeNames=["All"],
    )["Messages"]
    assert len(messages) == 1
    message = messages[0]
    message_attrs = message.get("MessageAttributes")
    assert message_attrs["action"].get("StringValue") == "ADDED"
    assert json.loads(message["Body"]).get("id") == nrt_dsid

    assert dc.index.datasets.get(final_dsid) is None

    final_result = runner.invoke(
        fs_cli,
        [
            str(TEST_DATA_FOLDER),
            "--glob=**/maturity-final.odc-metadata.yaml",
            "--archive-less-mature",
            f"--publish-action={sns_arn}",
        ],
        catch_exceptions=False,
    )
    print(
        f"fs-to-dc exit_code: {final_result.exit_code}, " "output:{final_result.output}"
    )

    assert final_result.exit_code == 0
    assert dc.index.datasets.get(final_dsid) is not None

    messages = sqs.receive_message(
        QueueUrl=queue_url,
        MaxNumberOfMessages=10,
        MessageAttributeNames=["All"],
    )["Messages"]
    assert len(messages) == 2

    nrt_message = messages[0]
    assert nrt_message.get("MessageAttributes")["action"].get("StringValue") == "ARCHIVED"
    assert json.loads(nrt_message["Body"]).get("id") == nrt_dsid

    final_message = messages[1]
    assert final_message.get("MessageAttributes")["action"].get("StringValue") == "ADDED"
    assert json.loads(final_message["Body"]).get("id") == final_dsid
