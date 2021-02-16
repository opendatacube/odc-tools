import pathlib
import pytest
from mock import MagicMock
import boto3
from moto import mock_sqs
from . import DummyPlugin

TEST_DIR = pathlib.Path(__file__).parent.absolute()


@pytest.fixture
def test_dir():
    return TEST_DIR


@pytest.fixture
def test_db_path(test_dir):
    return str(test_dir / "test_tiles.db")


@pytest.fixture
def dummy_plugin_name():
    from odc.stats._plugins import register

    name = "dummy-plugin"
    register(name, DummyPlugin)
    return name


@pytest.fixture
def sqs_message():
    response = {
        "ResponseMetadata": {
            "RequestId": "45ff2253-2bfe-5395-9f14-7af67a6b8f27",
            "HTTPStatusCode": 200,
            "HTTPHeaders": {
                "x-amzn-requestid": "45ff2253-2bfe-5395-9f14-7af67a6b8f27",
                "date": "Tue, 16 Feb 2021 04:51:33 GMT",
                "content-type": "text/xml",
                "content-length": "215",
            },
            "RetryAttempts": 0,
        }
    }

    msg = MagicMock()
    msg.delete = lambda: response
    msg.change_visibility = lambda VisibilityTimeout=0: response
    msg.body = ""
    return msg


@pytest.fixture
def sqs_queue_by_name():
    qname = "test-sqs"
    with mock_sqs():
        sqs = boto3.resource("sqs")
        sqs.create_queue(QueueName=qname)

        yield qname
