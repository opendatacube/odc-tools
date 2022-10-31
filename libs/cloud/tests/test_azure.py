"""Test thredds downloader code
"""
from odc.azure import find_blobs, download_yamls


def test_find_blobs():
    """Find blobs in a sample Azure account, this will fail if the blob store changes or is removed"""

    account_name = "geoau"
    account_url = "https://" + account_name + ".blob.core.windows.net"
    container_name = "ey-gsa"
    credential = "sv=2019-10-10&si=ey-gsa-ro&sr=c&sig=4T2mncGZ2v%2FqVsjjyHp%2Fv7BZytih8b251pSW0QelT98%3D"
    suffix = "odc-metadata.yaml"
    prefix = "baseline/ga_ls7e_ard_3/092/087/2018/05/25"

    blob_names = list(
        find_blobs(container_name, credential, prefix, suffix, account_url=account_url)
    )
    assert blob_names
    assert len(blob_names) == 1


def test_download_yamls():
    """Test pass/fail arms of YAML download from Azure blobstore"""

    account_name = "geoau"
    account_url = "https://" + account_name + ".blob.core.windows.net"
    container_name = "ey-gsa"
    credential = "sv=2019-10-10&si=ey-gsa-ro&sr=c&sig=4T2mncGZ2v%2FqVsjjyHp%2Fv7BZytih8b251pSW0QelT98%3D"

    test_blob_names = [
        "baseline/ga_ls7e_ard_3/092/087/2018/05/25/ga_ls7e_ard_3-0-0_092087_2018-05-25_final.odc-metadata.yaml"
    ]

    results = download_yamls(account_url, container_name, credential, test_blob_names)
    assert results
    assert len(results) == 1
    print(results)
    assert results[0][0] is not None
