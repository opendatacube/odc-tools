# Tests using the Click framework the s3-to-dc CLI tool
# flake8: noqa

from click.testing import CliRunner

from odc.apps.dc_tools.s3_to_dc import cli as s3_to_dc


def test_s3_to_dc_skips_already_indexed_datasets(
    mocked_s3_datasets, odc_test_db_with_products
):
    runner = CliRunner()
    # This will fail if requester pays is enabled
    results = [
        runner.invoke(
            s3_to_dc,
            [
                "--no-sign-request",
                "s3://odc-tools-test/cemp_insar/**/*.yaml",
                "cemp_insar_alos_displacement",
            ],
        )
        for _ in range(1, 3)
    ]

    # The first run should succeed and index all 25 datasets
    assert results[0].exit_code == 0
    assert (
        results[0].output
        == "Added 25 datasets, skipped 0 datasets and failed 0 datasets.\n"
    )

    # The second run should succeed by SKIPPING all 25 datasets
    assert results[1].exit_code == 0
    assert (
        results[1].output
        == "Added 0 datasets, skipped 25 datasets and failed 0 datasets.\n"
    )


def test_s3_to_dc_stac(mocked_s3_datasets, aws_env, odc_test_db_with_products):
    result = CliRunner().invoke(
        s3_to_dc,
        [
            "--no-sign-request",
            "--stac",
            "s3://odc-tools-test/sentinel-s2-l2a-cogs/31/Q/GB/2020/8/S2B_31QGB_20200831_0_L2A/*_L2A.json",
            "s2_l2a",
        ],
        catch_exceptions=False,
    )
    assert result.exit_code == 0
    assert (
        result.output == "Added 1 datasets, skipped 0 datasets and failed 0 datasets.\n"
    )


def test_s3_to_dc_stac_update_if_exist(mocked_s3_datasets, odc_test_db_with_products):
    result = CliRunner().invoke(
        s3_to_dc,
        [
            "--no-sign-request",
            "--stac",
            "--update-if-exists",
            "s3://odc-tools-test/sentinel-s2-l2a-cogs/31/Q/GB/2020/8/S2B_31QGB_20200831_0_L2A/*_L2A.json",
            "s2_l2a",
        ],
    )
    assert result.exit_code == 0
    assert (
        result.output == "Added 1 datasets, skipped 0 datasets and failed 0 datasets.\n"
    )


def test_s3_to_dc_stac_update_if_exist_allow_unsafe(
    mocked_s3_datasets, odc_test_db_with_products
):
    runner = CliRunner()
    result = runner.invoke(
        s3_to_dc,
        [
            "--no-sign-request",
            "--stac",
            "--update-if-exists",
            "--allow-unsafe",
            "s3://odc-tools-test/sentinel-s2-l2a-cogs/31/Q/GB/2020/8/S2B_31QGB_20200831_0_L2A/*_L2A.json",
            "s2_l2a",
        ],
    )
    print(f"s3-to-dc exit_code: {result.exit_code}, output:{result.output}")
    assert result.exit_code == 0
    assert (
        result.output == "Added 1 datasets, skipped 0 datasets and failed 0 datasets.\n"
    )


def test_s3_to_dc_fails_to_index_non_dataset_yaml(
    mocked_s3_datasets, odc_test_db_with_products
):
    runner = CliRunner()
    result = runner.invoke(
        s3_to_dc,
        [
            "--no-sign-request",
            # absolute single file s3 uri won't work with s3-to-dc, only uri string contain *
            # absolute path = "s3://dea-public-data/derivative/ga_ls5t_nbart_gm_cyear_3/3-0-0/x08/y23/1994--P1Y/ga_ls5t_nbart_gm_cyear_3_x08y23_1994--P1Y_final.proc-info.yaml",
            "s3://odc-tools-test/derivative/ga_ls5t_nbart_gm_cyear_3/3-0-0/x08/y23/1994--P1Y/*.proc-info.yaml",
            "ga_ls5t_nbart_gm_cyear_3",
        ],
        catch_exceptions=False,
    )
    assert result.exit_code == 1
    assert (
        result.output == "Added 0 datasets, skipped 0 datasets and failed 1 datasets.\n"
    )


def test_s3_to_dc_partially_succeeds_when_given_invalid_and_valid_dataset_yamls(
    mocked_s3_datasets, odc_test_db_with_products
):
    runner = CliRunner()
    result = runner.invoke(
        s3_to_dc,
        [
            "--no-sign-request",
            "--skip-lineage",
            # This folder contains two yaml one valid dataset yaml and another non dataset yaml
            "s3://odc-tools-test/derivative/ga_ls5t_nbart_gm_cyear_3/3-0-0/x08/y23/1994--P1Y/*.yaml",
            "ga_ls5t_nbart_gm_cyear_3",
        ],
    )
    assert result.exit_code == 1
    assert (
        result.output == "Added 1 datasets, skipped 0 datasets and failed 1 datasets.\n"
    )
