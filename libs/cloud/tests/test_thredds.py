"""Test thredds downloader code
"""

import pytest
from odc.thredds import download_yamls, thredds_find_glob


# It's too slow to fail, disabling this for now
@pytest.mark.skip
@pytest.mark.xfail
def test_thredds_crawl():
    """Crawl a sample Thredds URL, this will fail if NCI loses this data
    or Thredds is down
    """
    thredds_catalog = "http://dapds00.nci.org.au/thredds/catalog/if87/2018-11-29/"
    select = [".*ARD-METADATA.yaml"]
    skips = [".*NBAR.*", ".*SUPPLEMENTARY.*", ".*NBART.*", ".*/QA/.*"]
    urls = thredds_find_glob(thredds_catalog, skips, select)
    assert urls
    assert len(urls) == 490


@pytest.mark.skip
@pytest.mark.xfail
def test_download_yaml():
    """Test pass/fail arms of YAML download from Thredds"""
    test_urls = [
        "http://dapds00.nci.org.au/thredds/fileServer/if87/2018-11-29/S2A_OPER_"
        "MSI_ARD_TL_EPAE_20181129T012952_A017945_T56LLM_N02.07/ARD-METADATA.yaml",
        "http://dapds00.nci.org.au/thredds/fileServer/if87/2028-11-29/S2A_OPER_MSI_"
        "ARD_TL_EPAE_20281129T012952_A017945_T56LLM_N02.07/ARD-METADATA.yaml",
        "http://downtime00.nci.org.au/thredds/fileServer/if87/2018-11-29/S2A_OPER_"
        "MSI_ARD_TL_EPAE_20181129T012952_A017945_T56LLM_N02.07/ARD-METADATA.yaml",
    ]
    results = download_yamls(test_urls)
    assert results
    assert len(results) == 3
    print(results)
    assert results[0][0] is not None
    assert results[1][0] is None
    assert results[1][2] == "Yaml not found"
    assert results[2][2] == "Thredds Failed"
