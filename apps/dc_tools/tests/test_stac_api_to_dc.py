# Tests using the Click framework the stac_api-to-dc CLI tool
import pytest
from click.testing import CliRunner
from odc.apps.dc_tools.stac_api_to_dc import cli
from odc.apps.dc_tools.utils import MICROSOFT_PC_STAC_URI


@pytest.mark.xfail(reason="Earth Search API has changed and now this is failing too")
def test_stac_to_dc_earthsearch(odc_test_db_with_products):
    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "--catalog-href=https://earth-search.aws.element84.com/v0/",
            "--bbox=5,15,10,20",
            "--limit=10",
            "--collections=sentinel-s2-l2a-cogs",
            "--datetime=2020-08-01/2020-08-31",
        ],
        catch_exceptions=False,
    )
    assert result.exit_code == 0
    assert "Added 10 Datasets, failed 0 Datasets, skipped 0 Datasets" in result.output


@pytest.mark.xfail(reason="Currently failing because the USGS STAC is not up to spec")
def test_stac_to_dc_usgs(odc_test_db_with_products):
    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "--catalog-href=https://ibhoyw8md9.execute-api.us-west-2.amazonaws.com/prod",
            "--bbox=5,15,10,20",
            "--limit=10",
            "--collections=landsat-c2l2-sr",
            "--datetime=2020-08-01/2020-08-31",
        ],
        catch_exceptions=False,
    )
    assert result.exit_code == 0


@pytest.mark.xfail(
    reason="Failing with error 'ConformanceClasses.ITEM_SEARCH not supported'"
)
def test_stac_to_dc_planetarycomputer(odc_test_db_with_products):
    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            f"--catalog-href={MICROSOFT_PC_STAC_URI}",
            "--limit=1",
            "--collections=nasadem",
        ],
    )
    assert result.exit_code == 0
