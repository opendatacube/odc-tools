# Tests using the Click framework the s3-to-dc CLI tool
# flake8: noqa

import pytest
from click.testing import CliRunner
from odc.apps.dc_tools.s3_to_dc import cli as s3_to_dc


@pytest.mark.depends(on=["add_products"])
def test_s3_to_dc_yaml(aws_env):
    runner = CliRunner()
    # This will fail if requester pays is enabled
    result = runner.invoke(
        s3_to_dc,
        [
            "--statsd-setting",
            "localhost:8125",
            "--no-sign-request",
            "s3://dea-public-data/cemp_insar/insar/displacement/alos/2010/**/*.yaml",
            "cemp_insar_alos_displacement",
        ],
    )
    assert result.exit_code == 0
    assert (
        result.output
        == "Added 25 datasets, skipped 0 datasets and failed 0 datasets.\n"
    )


@pytest.mark.depends(on=["add_products"])
def test_s3_to_dc_yaml_rerun(aws_env):
    runner = CliRunner()
    # This will fail if requester pays is enabled
    result = runner.invoke(
        s3_to_dc,
        [
            "--statsd-setting",
            "localhost:8125",
            "--no-sign-request",
            "s3://dea-public-data/cemp_insar/insar/displacement/alos/2010/**/*.yaml",
            "cemp_insar_alos_displacement",
        ],
    )
    assert result.exit_code == 0
    assert (
        result.output
        == "Added 0 datasets, skipped 25 datasets and failed 0 datasets.\n"
    )


@pytest.mark.depends(on=["add_products"])
def test_s3_to_dc_stac(aws_env):
    runner = CliRunner()
    # This will fail if requester pays is enabled
    result = runner.invoke(
        s3_to_dc,
        [
            "--statsd-setting",
            "localhost:8125",
            "--no-sign-request",
            "--stac",
            "s3://sentinel-cogs/sentinel-s2-l2a-cogs/42/T/UM/2022/1/S2A_42TUM_20220102_0_L2A/*_L2A.json",
            "s2_l2a",
        ],
    )
    assert result.exit_code == 0
    assert (
        result.output == "Added 1 datasets, skipped 0 datasets and failed 0 datasets.\n"
    )


@pytest.mark.depends(on=["add_products"])
def test_s3_to_dc_stac_update_if_exist(aws_env):
    runner = CliRunner()
    # This will fail if requester pays is enabled
    result = runner.invoke(
        s3_to_dc,
        [
            "--statsd-setting",
            "localhost:8125",
            "--no-sign-request",
            "--stac",
            "--update-if-exists",
            "s3://sentinel-cogs/sentinel-s2-l2a-cogs/42/T/UM/2022/1/S2A_42TUM_20220102_0_L2A/*_L2A.json",
            "s2_l2a",
        ],
    )
    assert result.exit_code == 0
    assert (
        result.output == "Added 1 datasets, skipped 0 datasets and failed 0 datasets.\n"
    )


@pytest.mark.depends(on=["add_products"])
def test_s3_to_dc_stac_update_if_exist_allow_unsafe(aws_env):
    runner = CliRunner()
    # This will fail if requester pays is enabled
    result = runner.invoke(
        s3_to_dc,
        [
            "--statsd-setting",
            "localhost:8125",
            "--no-sign-request",
            "--stac",
            "--update-if-exists",
            "--allow-unsafe",
            "s3://sentinel-cogs/sentinel-s2-l2a-cogs/42/T/UM/2022/1/S2A_42TUM_20220102_0_L2A/*_L2A.json",
            "s2_l2a",
        ],
    )
    assert result.exit_code == 0
    assert (
        result.output == "Added 1 datasets, skipped 0 datasets and failed 0 datasets.\n"
    )


@pytest.mark.depends(on=["add_products"])
def test_s3_to_dc_single_glob_proc_info_yaml(aws_env):
    runner = CliRunner()
    # This will fail if requester pays is enabled
    result = runner.invoke(
        s3_to_dc,
        [
            "--no-sign-request",
            # absolute single file s3 uri won't work with s3-to-dc, only uri string contain *
            # absolute path = "s3://dea-public-data/derivative/ga_ls5t_nbart_gm_cyear_3/3-0-0/x08/y23/1994--P1Y/ga_ls5t_nbart_gm_cyear_3_x08y23_1994--P1Y_final.proc-info.yaml",
            "s3://dea-public-data/derivative/ga_ls5t_nbart_gm_cyear_3/3-0-0/x08/y23/1994--P1Y/*.proc-info.yaml",
            "ga_ls5t_nbart_gm_cyear_3",
        ],
    )
    assert result.exit_code == 1
    assert (
        result.output == "Added 0 datasets, skipped 0 datasets and failed 1 datasets.\n"
    )


@pytest.mark.depends(on=["add_products"])
def test_s3_to_dc_index_proc_info_yaml(aws_env):
    runner = CliRunner()
    # This will fail if requester pays is enabled
    result = runner.invoke(
        s3_to_dc,
        [
            "--no-sign-request",
            "--skip-lineage",
            # This folder contains two yaml one valid dataset yaml and another non dataset yaml
            "s3://dea-public-data/derivative/ga_ls5t_nbart_gm_cyear_3/3-0-0/x08/y23/1994--P1Y/*.yaml",
            "ga_ls5t_nbart_gm_cyear_3",
        ],
    )
    assert result.exit_code == 1
    assert (
        result.output == "Added 1 datasets, skipped 0 datasets and failed 1 datasets.\n"
    )
