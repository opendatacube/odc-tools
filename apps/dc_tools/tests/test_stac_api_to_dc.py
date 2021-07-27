# Tests using the Click framework the stac_api-to-dc CLI tool
from click.testing import CliRunner
from odc.apps.dc_tools.stac_api_to_dc import cli
import pytest


@pytest.mark.skip(
    reason="Don't run this, because it needs version 0.3.0 of sat-search, which breaks the build."
)
def test_s3_to_dc_stac():
    runner = CliRunner()
    # This will fail if requester pays is enabled
    result = runner.invoke(
        cli,
        [
            "--bbox=5,15,10,20",
            "--limit=10",
            "--collections=sentinel-s2-l2a-cogs",
            "--datetime=2020-08-01/2020-08-31",
        ],
    )
    assert result.exit_code == 0
    assert result.output == "Added 10 Datasets, failed 0 Datasets\n"
