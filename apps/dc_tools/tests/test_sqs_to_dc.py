"""
Test for SQS to DC tool
"""
import json
from functools import partial
from pprint import pformat

import pytest

import boto3
from moto import mock_sqs

from pathlib import Path

from datacube.utils import documents
from deepdiff import DeepDiff
from odc.apps.dc_tools._stac import stac_transform
from odc.apps.dc_tools.sqs_to_dc import (
    handle_json_message,
    handle_bucket_notification_message,
    extract_metadata_from_message,
    cli,
)
from click.testing import CliRunner
from datacube import Datacube
from datacube.index.hl import Doc2Dataset
from odc.aws.queue import get_messages

record_message = {
    "Records":[
        {
            "eventVersion":"2.1",
            "eventSource":"aws:s3",
            "awsRegion":"us-east-2",
            "eventTime":"2018-12-19T01:51:03.251Z",
            "eventName":"ObjectCreated:Put",
            "userIdentity":{
                "principalId":"AWS:AIDAIZLCFC5TZD36YHNZY"
            },
            "requestParameters":{
                "sourceIPAddress":"52.46.82.38"
            },
            "responseElements":{
                "x-amz-request-id":"6C05F1340AA50D21",
                "x-amz-id-2":"9e8KovdAUJwmYu1qnEv+urrO8T0vQ+UOpkPnFYLE6agmJSn745/T3/tVs0Low/vXonTdATvW23M="
            },
            "s3":{
                "s3SchemaVersion":"1.0",
                "configurationId":"test_SQS_Notification_1",
                "bucket":{
                    "name":"dea-public-data",
                    "ownerIdentity":{
                        "principalId":"A2SGQBYRFBZET"
                    },
                    "arn":"arn:aws:s3:::dea-public-data"
                },
                "object":{
                    "key":"cemp_insar/insar/displacement/alos/2009/06/17/alos_cumul_2009-06-17.yaml",
                    "size":713,
                    "eTag":"1ff1209e4140b4ff7a9d2b922f57f486",
                    "sequencer":"005C19A40717D99642"
                }
            }
        }
    ]
}

sqs_message = {
    "Type" : "Notification",
    "MessageId" : "xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxxxxxxxxx",
    "TopicArn" : "arn:aws:sns:ap-southeast-2:xxxxxxxxxxxxxxxxx:DEANewData",
    "Subject" : "Amazon S3 Notification",
    "Message": json.dumps(record_message),
    "Timestamp" : "2020-08-21T08:28:45.921Z",
    "SignatureVersion" : "1",
    "Signature" : "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx",
    "SigningCertURL" : "https://sns.ap-southeast-2.amazonaws.com/SimpleNotificationService-xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx.pem",
    "UnsubscribeURL" : "https://sns.ap-southeast-2.amazonaws.com/?Action=Unsubscribe&SubscriptionArn=arn:aws:sns:ap-southeast-2:xxxxxxxxxxxxxxx:DEANewData:xxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxxxx"
}



TEST_DATA_FOLDER: Path = Path(__file__).parent.joinpath("data")
LANDSAT_C3_SQS_MESSAGE: str = "ga_ls8c_ard_3-1-0_088080_2020-05-25_final.stac-item.sqs.json"
LANDSAT_C3_ODC_YAML: str = "ga_ls8c_ard_3-1-0_088080_2020-05-25_final.odc-metadata.sqs.yaml"
SENTINEL_2_NRT_MESSAGE: str = "sentinel-2-nrt_2020_08_21.json"

deep_diff = partial(
    DeepDiff, significant_digits=6, ignore_type_in_groups=[(tuple, list)]
)


def test_extract_metadata_from_message():
    TEST_QUEUE_NAME = "a_test_queue"
    sqs_resource = boto3.resource("sqs")
    dc = Datacube()

    a_queue = sqs_resource.create_queue(QueueName=TEST_QUEUE_NAME)

    a_queue.send_message(MessageBody=json.dumps(sqs_message))
    assert int(a_queue.attributes.get("ApproximateNumberOfMessages")) == 1

    assert dc.index.datasets.get("69a6eca2-ca45-4808-a5b3-694029200c43") is None

    queue = sqs_resource.get_queue_by_name(QueueName=TEST_QUEUE_NAME)

    for m in get_messages(queue):
        metadata = extract_metadata_from_message(m)
        data, uri = handle_bucket_notification_message(
            m, metadata, "cemp_insar/insar/displacement/alos/*", True
        )

        assert uri == "s3://dea-public-data/cemp_insar/insar/displacement/alos/2009/06/17/alos_cumul_2009-06-17.yaml"
        assert type(data)  == dict

        doc2ds = Doc2Dataset(dc.index, products=['cemp_insar_alos_displacement'])
        from odc.apps.dc_tools.utils import index_update_dataset
        index_update_dataset(
            data,
            uri,
            dc,
            doc2ds,
        )

        assert dc.index.datasets.get("69a6eca2-ca45-4808-a5b3-694029200c43") is not None
        m.delete()


def test_hand_bucket_notification_message():
    data, uri = handle_bucket_notification_message(
        sqs_message, record_message, "cemp_insar/insar/displacement/alos/*"
    )

    assert uri == "s3://dea-public-data/cemp_insar/insar/displacement/alos/2009/06/17/alos_cumul_2009-06-17.yaml"
    assert type(data)  == dict


def test_handle_json_message(ga_ls8c_ard_3_message, ga_ls8c_ard_3_yaml):
    actual_doc, uri = handle_json_message(
        ga_ls8c_ard_3_message, None, "STAC-LINKS-REL:odc_yaml"
    )

    assert type(actual_doc) is dict
    assert ga_ls8c_ard_3_yaml["id"] == actual_doc["id"]
    assert ga_ls8c_ard_3_yaml["crs"] == actual_doc["crs"]
    assert ga_ls8c_ard_3_yaml["product"]["name"] == actual_doc["product"]["name"]
    assert ga_ls8c_ard_3_yaml["label"] == actual_doc["label"]

    # Test geometry field
    doc_diff = deep_diff(ga_ls8c_ard_3_yaml["geometry"], actual_doc["geometry"])
    assert doc_diff == {}, pformat(doc_diff)

    # Test grids field
    doc_diff = deep_diff(
        ga_ls8c_ard_3_yaml["grids"]["default"], actual_doc["grids"]["default"]
    )
    assert doc_diff == {}, pformat(doc_diff)

    # Test measurements field
    doc_diff = deep_diff(
        ga_ls8c_ard_3_yaml["measurements"],
        actual_doc["measurements"],
        exclude_paths=["root['nbart_panchromatic']"],
    )
    assert doc_diff == {}, pformat(doc_diff)


def test_odc_metadata_link(ga_ls8c_ard_3_message):
    actual_doc, uri = handle_json_message(
        ga_ls8c_ard_3_message, None, "STAC-LINKS-REL:odc_yaml"
    )
    assert (
        uri == "http://dea-public-data.s3-ap-southeast-2.amazonaws.com/"
        "baseline/ga_ls8c_ard_3/088/080/2020/05/25/"
        "ga_ls8c_ard_3-1-0_088080_2020-05-25_final.odc-metadata.yaml"
    )


def test_stac_link(ga_ls8c_ard_3_message):
    metadata, uri = handle_json_message(ga_ls8c_ard_3_message, stac_transform, None)
    assert (
        uri != "http://dea-public-data.s3-ap-southeast-2.amazonaws.com/"
        "baseline/ga_ls8c_ard_3/088/080/2020/05/25/"
        "ga_ls8c_ard_3-1-0_088080_2020-05-25_final.odc-metadata.yaml"
    )
    assert (
        uri == "http://dea-public-data.s3-ap-southeast-2.amazonaws.com/"
        "baseline/ga_ls8c_ard_3/088/080/2020/05/25/"
        "ga_ls8c_ard_3-1-0_088080_2020-05-25_final.stac-item.json"
    )


def test_transform(ga_ls8c_ard_3_message, ga_ls8c_ard_3_yaml):
    actual_doc, uri = handle_json_message(ga_ls8c_ard_3_message, stac_transform, None)

    assert ga_ls8c_ard_3_yaml["id"] == actual_doc["id"]
    assert ga_ls8c_ard_3_yaml["crs"] == actual_doc["crs"]
    assert ga_ls8c_ard_3_yaml["product"]["name"] == actual_doc["product"]["name"]
    assert ga_ls8c_ard_3_yaml["label"] == actual_doc["label"]

    # Test geometry field
    # TODO: fix geometry test here.
    # doc_diff = deep_diff(ga_ls8c_ard_3_yaml["geometry"], actual_doc["geometry"])
    # assert doc_diff == {}, pformat(doc_diff)

    # Test grids field
    doc_diff = deep_diff(ga_ls8c_ard_3_yaml["grids"], actual_doc["grids"])
    assert doc_diff == {}, pformat(doc_diff)

    # Test measurements field
    doc_diff = deep_diff(ga_ls8c_ard_3_yaml["measurements"], actual_doc["measurements"])
    assert doc_diff == {}, pformat(doc_diff)

    # Test properties field
    doc_diff = deep_diff(
        ga_ls8c_ard_3_yaml["properties"],
        actual_doc["properties"],
        exclude_paths=[
            "root['odc:product']",
            "root['datetime']",
            "root['dtr:start_datetime']",
            "root['dtr:end_datetime']",
            "root['odc:processing_datetime']",
            "root['proj:epsg']",
            "root['proj:shape']",
            "root['proj:transform']",
        ],
    )
    assert doc_diff == {}, pformat(doc_diff)


@pytest.fixture
def ga_ls8c_ard_3_message():
    with TEST_DATA_FOLDER.joinpath(LANDSAT_C3_SQS_MESSAGE).open("r") as f:
        body = json.load(f)
    metadata = json.loads(body["Message"])
    return metadata


@pytest.fixture
def ga_ls8c_ard_3_yaml():
    metadata = yield from documents.load_documents(
        TEST_DATA_FOLDER.joinpath(LANDSAT_C3_ODC_YAML)
    )
    return metadata
