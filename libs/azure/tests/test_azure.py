"""Test thredds downloader code
"""
import os

import pytest
from odc.azure import download_yamls, find_blobs


@pytest.mark.skip("Takes too long to run, enable manually")
def test_fail_blob(credential, test_blob_names):
    """Search for a non-existant blob"""

    account_name = "not-real"
    account_url = "https://" + account_name + ".blob.core.windows.net"
    container_name = "fake"
    suffix = "odc-metadata.yaml"
    prefix = "fake/path"

    blob_names = find_blobs(account_url, container_name, credential, prefix, suffix)
    assert len(blob_names) == 0

    results = download_yamls(account_url, container_name, credential, test_blob_names)
    assert results


def test_find_blobs(credential):
    """Find blobs in a sample Azure account, this will fail if the blob store changes or is removed"""

    account_name = "geoau"
    account_url = "https://" + account_name + ".blob.core.windows.net"
    container_name = "ey-gsa"
    suffix = "odc-metadata.yaml"
    prefix = "baseline/ga_ls7e_ard_3/092/087/2018/05/25"

    blob_names = find_blobs(account_url, container_name, credential, prefix, suffix)
    assert blob_names
    assert len(blob_names) == 1


def test_download_yamls(credential, test_blob_names):
    """Test pass/fail arms of YAML download from Azure blobstore"""

    account_name = "geoau"
    account_url = "https://" + account_name + ".blob.core.windows.net"
    container_name = "ey-gsa"

    results = download_yamls(account_url, container_name, credential, test_blob_names)
    assert results
    assert len(results) == 1
    assert results[0][0] is not None


@pytest.fixture
def credential():
    return os.environ.get("AZURE_STORAGE_SAS_TOKEN")


@pytest.fixture
def test_blob_names():
    return [
        "baseline/ga_ls7e_ard_3/092/087/2018/05/25/ga_ls7e_ard_3-0-0_092087_2018-05-25_final.odc-metadata.yaml"
    ]
