# Tests using the Click framework the s3-to-dc CLI tool

import pytest
from click.testing import CliRunner
from odc.apps.dc_tools.s3_to_dc import cli


@pytest.mark.depends(on=['add_products'])
def test_s3_to_dc_statsd(aws_env):
    runner = CliRunner()
    # This will fail if requester pays is enabled
    result = runner.invoke(
        cli,
        [
            "--statsd-setting",
            "localhost:8125",
            "--no-sign-request",
            "--stac",
            "--update-if-exists",
            "--allow-unsafe",
            "s3://sentinel-cogs/sentinel-s2-l2a-cogs/42/T/UM/2022/1/S2A_42TUM_20220102_0_L2A/*.json",
            "s2_l2a",
        ],
    )
    assert result.exit_code == 0
    assert result.output == "Added 1 datasets and failed 0 datasets.\n"
