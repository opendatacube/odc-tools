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
    """Set up SNS topic and SQS queue subscribed to it

    Tests are structured as follows:
    input: [ STAC -> SNS -> SQS ] -> dc_tools -> output: [ STAC -> SNS -> SQS ]
    """
    with mock_sqs(), mock_sns():
        sns = boto3.client("sns")
        sqs = boto3.client("sqs")

        # Set up input topic and queue which dc_tools will receive from
        input_topic = sns.create_topic(Name="input-topic")
        input_topic_arn = input_topic.get("TopicArn")

        input_queue_name = "input-queue"
        input_queue = sqs.create_queue(QueueName=input_queue_name)
        input_attrs = sqs.get_queue_attributes(
            QueueUrl=input_queue.get("QueueUrl"), AttributeNames=["QueueArn"]
        )
        input_queue_arn = input_attrs["Attributes"]["QueueArn"]

        sns.subscribe(
            TopicArn=input_topic_arn,
            Protocol="sqs",
            Endpoint=input_queue_arn,
        )

        # Set up output topic and queue, which dc_tools will publish results to
        output_topic = sns.create_topic(Name="output-topic")
        output_topic_arn = output_topic.get("TopicArn")

        output_queue_name = "test-queue"
        output_queue = sqs.create_queue(QueueName=output_queue_name)
        output_attrs = sqs.get_queue_attributes(
            QueueUrl=output_queue.get("QueueUrl"), AttributeNames=["QueueArn"]
        )
        output_queue_arn = output_attrs["Attributes"]["QueueArn"]

        sns.subscribe(
            TopicArn=output_topic_arn,
            Protocol="sqs",
            Endpoint=output_queue_arn,
        )

        yield sns, input_topic_arn, output_topic_arn, sqs, input_queue_name, output_queue.get(
            "QueueUrl"
        )


def test_s3_publishing_action_from_stac(
    mocked_s3_datasets, odc_test_db_with_products, s2am_dsid, sns_setup
):
    _, _, output_topic_arn, sqs, _, output_queue_url = sns_setup

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
            f"--publish-action={output_topic_arn}",
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
        QueueUrl=output_queue_url,
    )["Messages"]
    assert len(messages) == 1
    message_attrs = json.loads(messages[0]["Body"]).get("MessageAttributes")
    assert message_attrs["action"].get("Value") == "ADDED"
    assert message_attrs["product"].get("Value") == "ga_s2am_ard_3"


def test_s3_publishing_action_from_eo3(
    mocked_s3_datasets, odc_test_db_with_products, s2am_dsid, sns_setup
):
    """Same as above but requiring stac to eo3 conversion"""
    _, _, output_topic_arn, sqs, _, output_queue_url = sns_setup

    dc = odc_test_db_with_products
    assert dc.index.datasets.get(s2am_dsid) is None

    runner = CliRunner()
    result = runner.invoke(
        s3_cli,
        [
            "--no-sign-request",
            "--update-if-exists",
            "--skip-lineage",
            f"--publish-action={output_topic_arn}",
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
        QueueUrl=output_queue_url,
    )["Messages"]
    assert len(messages) == 1
    message_attrs = json.loads(messages[0]["Body"]).get("MessageAttributes")
    assert message_attrs["action"].get("Value") == "ADDED"


TEST_DATA_FOLDER: Path = Path(__file__).parent.joinpath("data")
STAC_DATA: str = "ga_ls5t_nbart_gm_cyear_3_x30y14_1999--P1Y_final.stac-item.json"


@pytest.fixture
def stac_doc():
    with TEST_DATA_FOLDER.joinpath(STAC_DATA).open("r") as f:
        return f.read()


def test_sqs_publishing(
    aws_credentials, aws_env, stac_doc, odc_test_db_with_products, sns_setup
):
    """Test that actions are published with sqs_to_dc"""
    (
        _,
        input_topic_arn,
        output_topic_arn,
        sqs,
        input_queue_name,
        output_queue_url,
    ) = sns_setup

    input_queue = sqs.create_queue(QueueName=input_queue_name)
    sqs.send_message(
        QueueUrl=input_queue.get("QueueUrl"),
        MessageBody=json.dumps(
            {
                "Type": "Notification",
                "MessageId": "xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxxxxxxxxx",
                "TopicArn": input_topic_arn,
                "Message": stac_doc,
                "MessageAttributes": {
                    "action": {"DataType": "String", "StringValue": "ARCHIVED"}
                },
            }
        ),
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
            f"--publish-action={output_topic_arn}",
        ],
        catch_exceptions=False,
    )
    print(f"sqs-to-dc exit_code: {result.exit_code}, output:{result.output}")

    assert result.exit_code == 0

    messages = sqs.receive_message(
        QueueUrl=output_queue_url,
    )["Messages"]
    assert len(messages) == 1
    message_attrs = json.loads(messages[0]["Body"]).get("MessageAttributes")
    assert message_attrs["action"].get("Value") == "ADDED"
    assert message_attrs["product"].get("Value") == "ga_ls5t_nbart_gm_cyear_3"
    assert message_attrs["maturity"].get("Value") == "final"


def test_sqs_publishing_archive_flag(
    aws_credentials, aws_env, stac_doc, odc_db_for_archive, ls5t_dsid, sns_setup
):
    """Test that an ARCHIVE SNS message is published when the --archive flag is used."""
    (
        sns,
        input_topic_arn,
        output_topic_arn,
        sqs,
        input_queue_name,
        output_queue_url,
    ) = sns_setup

    sns.publish(
        TopicArn=input_topic_arn,
        Message=stac_doc,
        MessageAttributes={"action": {"DataType": "String", "StringValue": "ARCHIVED"}},
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
            f"--publish-action={output_topic_arn}",
        ],
        catch_exceptions=False,
    )
    print(f"sqs-to-dc exit_code: {result.exit_code}, output:{result.output}")

    assert result.exit_code == 0

    messages = sqs.receive_message(
        QueueUrl=output_queue_url,
    )["Messages"]
    assert len(messages) == 1
    message_attrs = json.loads(messages[0]["Body"]).get("MessageAttributes")
    assert message_attrs["action"].get("Value") == "ARCHIVED"
    assert dc.index.datasets.get(ls5t_dsid).is_archived is True


def test_sqs_publishing_archive_attribute(
    aws_credentials, aws_env, stac_doc, odc_db_for_archive, ls5t_dsid, sns_setup
):
    """Test that archiving occurs when ARCHIVED is in the message attributes"""
    (
        sns,
        input_topic_arn,
        output_topic_arn,
        sqs,
        input_queue_name,
        output_queue_url,
    ) = sns_setup

    sns.publish(
        TopicArn=input_topic_arn,
        Message=stac_doc,
        MessageAttributes={"action": {"DataType": "String", "StringValue": "ARCHIVED"}},
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
            f"--publish-action={output_topic_arn}",
        ],
        catch_exceptions=False,
    )

    assert result.exit_code == 0
    messages = sqs.receive_message(
        QueueUrl=output_queue_url,
        MessageAttributeNames=["All"],
    )["Messages"]
    assert len(messages) == 1
    message_attrs = json.loads(messages[0]["Body"]).get("MessageAttributes")
    assert message_attrs["action"].get("Value") == "ARCHIVED"
    assert dc.index.datasets.get(ls5t_dsid).is_archived is True


def test_with_archive_less_mature(
    aws_credentials,
    aws_env,
    odc_db,
    test_data_dir,
    nrt_dsid,
    final_dsid,
    sns_setup,
):
    _, _, output_topic_arn, sqs, _, output_queue_url = sns_setup

    dc = odc_db
    assert dc.index.datasets.get(nrt_dsid) is None

    runner = CliRunner()
    nrt_result = runner.invoke(
        fs_cli,
        [
            test_data_dir,
            "--glob=**/maturity-nrt.odc-metadata.yaml",
            "--archive-less-mature",
            f"--publish-action={output_topic_arn}",
        ],
        catch_exceptions=False,
    )
    print(f"fs-to-dc exit_code: {nrt_result.exit_code}, " "output:{nrt_result.output}")

    assert nrt_result.exit_code == 0
    assert dc.index.datasets.get(nrt_dsid) is not None

    messages = sqs.receive_message(
        QueueUrl=output_queue_url,
    )["Messages"]
    assert len(messages) == 1
    message = messages[0]
    message_attrs = json.loads(message["Body"]).get("MessageAttributes")
    assert message_attrs["action"].get("Value") == "ADDED"
    body = json.loads(message["Body"])
    assert json.loads(body["Message"]).get("id") == nrt_dsid

    assert dc.index.datasets.get(final_dsid) is None

    final_result = runner.invoke(
        fs_cli,
        [
            test_data_dir,
            "--glob=**/maturity-final.odc-metadata.yaml",
            "--archive-less-mature",
            f"--publish-action={output_topic_arn}",
        ],
        catch_exceptions=False,
    )
    print(
        f"fs-to-dc exit_code: {final_result.exit_code}, " "output:{final_result.output}"
    )

    assert final_result.exit_code == 0
    assert dc.index.datasets.get(final_dsid) is not None

    messages = sqs.receive_message(
        QueueUrl=output_queue_url,
        MaxNumberOfMessages=10,
    )["Messages"]
    assert len(messages) == 2

    nrt_message = messages[0]
    nrt_attrs = json.loads(nrt_message["Body"]).get("MessageAttributes")
    assert nrt_attrs["action"].get("Value") == "ARCHIVED"

    nrt_body = json.loads(nrt_message["Body"])
    assert json.loads(nrt_body["Message"]).get("id") == nrt_dsid

    final_message = messages[1]
    final_attrs = json.loads(final_message["Body"]).get("MessageAttributes")
    assert final_attrs["action"].get("Value") == "ADDED"
    final_body = json.loads(final_message["Body"])
    assert json.loads(final_body["Message"]).get("id") == final_dsid
