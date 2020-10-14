"""
Test for SQS to DC tool
"""
import json
from functools import partial
from pprint import pformat

import pytest

from pathlib import Path

from datacube.utils import documents
from deepdiff import DeepDiff
from datetime import date
from odc.index.stac import stac_transform
from odc.apps.dc_tools.sqs_to_dc import (
    get_metadata_uri,
    get_metadata_from_s3_record,
    get_s3_url,
)


TEST_DATA_FOLDER: Path = Path(__file__).parent.joinpath("data")
LANDSAT_C3_SQS_MESSAGE: str = "ga_ls8c_ard_3-1-0_088080_2020-05-25_final.stac-item.json"
LANDSAT_C3_ODC_YAML: str = "ga_ls8c_ard_3-1-0_088080_2020-05-25_final.odc-metadata.yaml"
SENTINEL_2_NRT_MESSAGE: str = "sentinel-2-nrt_2020_08_21.json"
SENTINEL_2_NRT_RECORD_PATH = ("L2/sentinel-2-nrt/S2MSIARD/*/*/ARD-METADATA.yaml",)

deep_diff = partial(
    DeepDiff, significant_digits=6, ignore_type_in_groups=[(tuple, list)]
)

@pytest.mark.skip(reason="Test is failing due to requiring credentials")
@pytest.mark.skipif(
    date.today() > date(2020, 11, 10), reason="dataset has been rotated out"
)
def test_get_metadata_s3_object(sentinel_2_nrt_message, sentinel_2_nrt_record_path):
    data, uri = get_metadata_from_s3_record(
        sentinel_2_nrt_message, sentinel_2_nrt_record_path
    )

    assert type(data) is dict
    assert uri == get_s3_url(
        bucket_name="dea-public-data",
        obj_key="L2/sentinel-2-nrt/S2MSIARD/2020-08-21/S2B_OPER_MSI_ARD_TL_VGS1_20200821T014801_A018060_T54HVH_N02.09/ARD-METADATA.yaml",
    )


def test_get_metadata_uri(ga_ls8c_ard_3_message, ga_ls8c_ard_3_yaml):
    actual_doc, uri = get_metadata_uri(
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
    actual_doc, uri = get_metadata_uri(
        ga_ls8c_ard_3_message, None, "STAC-LINKS-REL:odc_yaml"
    )
    assert (
        uri == "http://dea-public-data-dev.s3-ap-southeast-2.amazonaws.com/"
        "analysis-ready-data/ga_ls8c_ard_3/088/080/2020/05/25/"
        "ga_ls8c_ard_3-1-0_088080_2020-05-25_final.odc-metadata.yaml"
    )


def test_stac_link(ga_ls8c_ard_3_message):
    metadata, uri = get_metadata_uri(ga_ls8c_ard_3_message, stac_transform, None)
    assert (
        uri != "http://dea-public-data-dev.s3-ap-southeast-2.amazonaws.com/"
        "analysis-ready-data/ga_ls8c_ard_3/088/080/2020/05/25/"
        "ga_ls8c_ard_3-1-0_088080_2020-05-25_final.odc-metadata.yaml"
    )
    assert (
        uri == "http://dea-public-data-dev.s3-ap-southeast-2.amazonaws.com/"
        "analysis-ready-data/ga_ls8c_ard_3/088/080/2020/05/25/"
        "ga_ls8c_ard_3-1-0_088080_2020-05-25_final.stac-item.json"
    )


@pytest.mark.skip(reason="Skipping due to issues with coordinate rounding")
def test_transform(ga_ls8c_ard_3_message, ga_ls8c_ard_3_yaml):
    actual_doc, uri = get_metadata_uri(ga_ls8c_ard_3_message, stac_transform, None)

    assert ga_ls8c_ard_3_yaml["id"] == actual_doc["id"]
    assert ga_ls8c_ard_3_yaml["crs"] == actual_doc["crs"]
    assert ga_ls8c_ard_3_yaml["product"]["name"] == actual_doc["product"]["name"]
    assert ga_ls8c_ard_3_yaml["label"] == actual_doc["label"]

    # Test geometry field
    doc_diff = deep_diff(ga_ls8c_ard_3_yaml["geometry"], actual_doc["geometry"])
    assert doc_diff == {}, pformat(doc_diff)

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
def sentinel_2_nrt_record_path():
    return SENTINEL_2_NRT_RECORD_PATH


@pytest.fixture
def sentinel_2_nrt_message():
    with TEST_DATA_FOLDER.joinpath(SENTINEL_2_NRT_MESSAGE).open("r") as f:
        body = json.load(f)
    metadata = json.loads(body["Message"])
    return metadata


@pytest.fixture
def ga_ls8c_ard_3_yaml():
    metadata = yield from documents.load_documents(
        TEST_DATA_FOLDER.joinpath(LANDSAT_C3_ODC_YAML)
    )
    return metadata
