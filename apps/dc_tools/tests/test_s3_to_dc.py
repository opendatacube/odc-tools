# Tests using the Click framework the s3-to-dc CLI tool

import pytest
from click.testing import CliRunner
from odc.apps.dc_tools.s3_to_dc import cli


@pytest.mark.depends(on=['add_products'])
def test_s3_to_dc_yaml(aws_env):
    runner = CliRunner()
    # This will fail if requester pays is enabled
    result = runner.invoke(
        cli,
        [
            "--no-sign-request",
            "s3://dea-public-data/cemp_insar/insar/displacement/alos/2010/**/*.yaml",
            "cemp_insar_alos_displacement",
        ],
    )
    assert result.exit_code == 0
    assert result.output == "Added 25 datasets and failed 0 datasets.\n"


@pytest.mark.depends(on=['add_products'])
def test_s3_to_dc_stac(aws_env):
    runner = CliRunner()
    # This will fail if requester pays is enabled
    result = runner.invoke(
        cli,
        [
            "--no-sign-request",
            "--stac",
            "s3://sentinel-cogs/sentinel-s2-l2a-cogs/42/T/UM/2022/1/S2A_42TUM_20220102_0_L2A/*.json",
            "s2_l2a",
        ],
    )
    assert result.exit_code == 0
    assert result.output == "Added 1 datasets and failed 0 datasets.\n"


def test_s3_to_dc_single_stac(aws_env):
    runner = CliRunner()
    # This will fail if requester pays is enabled
    result = runner.invoke(
        cli,
        [
            "--no-sign-request",
            "--stac",
            "s3://sentinel-cogs/sentinel-s2-l2a-cogs/42/T/UM/2022/1/S2B_42TUM_20220114_0_L2A/S2B_42TUM_20220114_0_L2A.json",
            "s2_l2a",
        ],
    )
    assert result.exit_code == 0
    assert result.output == "Added 1 datasets and failed 0 datasets.\n"


@pytest.mark.depends(on=['add_products'])
def test_s3_to_dc_stac_update_if_exist(aws_env):
    runner = CliRunner()
    # This will fail if requester pays is enabled
    result = runner.invoke(
        cli,
        [
            "--no-sign-request",
            "--stac",
            "--update-if-exists",
            "s3://sentinel-cogs/sentinel-s2-l2a-cogs/42/T/UM/2022/1/S2A_42TUM_20220102_0_L2A/*.json",
            "s2_l2a",
        ],
    )
    assert result.exit_code == 0
    assert result.output == "Added 1 datasets and failed 0 datasets.\n"


@pytest.mark.depends(on=['add_products'])
def test_s3_to_dc_stac_update_if_exist_allow_unsafe(aws_env):
    runner = CliRunner()
    # This will fail if requester pays is enabled
    result = runner.invoke(
        cli,
        [
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
