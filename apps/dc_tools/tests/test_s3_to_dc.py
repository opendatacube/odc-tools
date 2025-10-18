# Tests using the Click framework the s3-to-dc CLI tool
# flake8: noqa
import boto3
import json
import pytest
from moto import mock_aws
from click.testing import CliRunner
from odc.aws.queue import get_queue, get_queues, redrive_queue
from odc.apps.dc_tools.s3_to_dc import cli as s3_to_dc
from odc.apps.dc_tools.redrive_to_queue import cli as redrive_cli


ALIVE_QUEUE_NAME = "mock-alive-queue"
DEAD_QUEUE_NAME = "mock-dead-queue"


@mock_aws
def test_redrive_to_queue_cli(aws_env) -> None:
    resource = boto3.resource("sqs")

    dead_queue = resource.create_queue(QueueName=DEAD_QUEUE_NAME)
    resource.create_queue(
        QueueName=ALIVE_QUEUE_NAME,
        Attributes={
            "RedrivePolicy": json.dumps(
                {
                    "deadLetterTargetArn": dead_queue.attributes.get("QueueArn"),
                    "maxReceiveCount": 2,
                }
            ),
        },
    )

    for i in range(35):
        dead_queue.send_message(MessageBody=json.dumps({"content": f"Something {i}"}))
    runner = CliRunner()
    # Invalid value string
    returned = runner.invoke(
        redrive_cli,
        [str(DEAD_QUEUE_NAME), str(ALIVE_QUEUE_NAME), "--limit", "string_test"],
    )
    assert returned.exit_code == 1, f"Output: {returned.output}"

    # Invalid value 0
    returned = runner.invoke(
        redrive_cli,
        [str(DEAD_QUEUE_NAME), str(ALIVE_QUEUE_NAME), "--limit", 0],
    )
    assert returned.exit_code == 1, f"Output: {returned.output}"

    # Valid value 1
    returned = runner.invoke(
        redrive_cli,
        [str(DEAD_QUEUE_NAME), str(ALIVE_QUEUE_NAME), "--limit", 1],
    )
    assert returned.exit_code == 0, f"Output: {returned.output}"
    assert (
        int(get_queue(ALIVE_QUEUE_NAME).attributes.get("ApproximateNumberOfMessages"))
        == 1
    )

    # Valid value None (all)
    returned = runner.invoke(
        redrive_cli,
        [str(DEAD_QUEUE_NAME), str(ALIVE_QUEUE_NAME), "--limit", None],
    )
    assert returned.exit_code == 0, f"Output: {returned.output}"
    assert (
        int(get_queue(DEAD_QUEUE_NAME).attributes.get("ApproximateNumberOfMessages"))
        == 0
    )


def test_s3_to_dc_skips_already_indexed_datasets(
    mocked_s3_datasets, odc_test_db_with_products, env_name
) -> None:
    runner = CliRunner()
    # This will fail if requester pays is enabled
    results = [
        runner.invoke(
            s3_to_dc,
            [
                "--no-sign-request",
                "s3://odc-tools-test/cemp_insar/**/*.yaml",
                "cemp_insar_alos_displacement",
                "--env",
                env_name,
            ],
        )
        for _ in range(1, 3)
    ]

    # The first run should succeed and index all 25 datasets
    assert results[0].exit_code == 0, f"Output: {results[0].output}"
    assert (
        results[0].output
        == "Added 25 datasets, skipped 0 datasets and failed 0 datasets.\n"
    )

    # The second run should succeed by SKIPPING all 25 datasets
    assert results[1].exit_code == 0, f"Output: {results[1].output}"
    assert (
        results[1].output
        == "Added 0 datasets, skipped 25 datasets and failed 0 datasets.\n"
    )


def test_s3_to_dc_stac(
    mocked_s3_datasets, aws_env, odc_test_db_with_products, env_name
) -> None:
    result = CliRunner().invoke(
        s3_to_dc,
        [
            "--no-sign-request",
            "--stac",
            "s3://odc-tools-test/sentinel-s2-l2a-cogs/31/Q/GB/2020/8/S2B_31QGB_20200831_0_L2A/*_L2A.json",
            "--rename-product",
            "s2_l2a",
            "--env",
            env_name,
        ],
        catch_exceptions=False,
    )
    assert result.exit_code == 0, f"Output: {result.output}"
    assert (
        result.output == "Added 1 datasets, skipped 0 datasets and failed 0 datasets.\n"
    )


def test_s3_to_dc_stac_update_if_exist(
    mocked_s3_datasets, odc_test_db_with_products, env_name
) -> None:
    result = CliRunner().invoke(
        s3_to_dc,
        [
            "--no-sign-request",
            "--stac",
            "--update-if-exists",
            "s3://odc-tools-test/sentinel-s2-l2a-cogs/31/Q/GB/2020/8/S2B_31QGB_20200831_0_L2A/*_L2A.json",
            "--rename-product",
            "s2_l2a",
            "--env",
            env_name,
        ],
    )
    assert result.exit_code == 0, f"Output: {result.output}"
    assert (
        result.output == "Added 1 datasets, skipped 0 datasets and failed 0 datasets.\n"
    )


def test_s3_to_dc_stac_update_if_exist_allow_unsafe(
    mocked_s3_datasets, odc_test_db_with_products, env_name
) -> None:
    runner = CliRunner()
    result = runner.invoke(
        s3_to_dc,
        [
            "--no-sign-request",
            "--stac",
            "--update-if-exists",
            "--allow-unsafe",
            "s3://odc-tools-test/sentinel-s2-l2a-cogs/31/Q/GB/2020/8/S2B_31QGB_20200831_0_L2A/*_L2A.json",
            "--rename-product",
            "s2_l2a",
            "--env",
            env_name,
        ],
    )
    print(f"s3-to-dc exit_code: {result.exit_code}, output:{result.output}")
    assert result.exit_code == 0, f"Output: {result.output}"
    assert (
        result.output == "Added 1 datasets, skipped 0 datasets and failed 0 datasets.\n"
    )


def test_s3_to_dc_fails_to_index_non_dataset_yaml(
    mocked_s3_datasets, odc_test_db_with_products, env_name
) -> None:
    runner = CliRunner()
    result = runner.invoke(
        s3_to_dc,
        [
            "--no-sign-request",
            "s3://odc-tools-test/baseline/ga_s2am_ard_3/49/JFM/2016/12/14/20161214T092514/ga_s2am_ard_3-2-1_49JFM_2016-12-14_final.odc-metadata.yaml",
            "ga_ls5t_nbart_gm_cyear_3",
            "--env",
            env_name,
        ],
        catch_exceptions=False,
    )
    assert result.exit_code == 1, f"Output: {result.output}"
    assert (
        result.output == "Added 0 datasets, skipped 0 datasets and failed 1 datasets.\n"
    )


def test_s3_to_dc_partially_succeeds_when_given_invalid_and_valid_dataset_yamls(
    mocked_s3_datasets, odc_test_db_with_products, env_name
) -> None:
    runner = CliRunner()
    result = runner.invoke(
        s3_to_dc,
        [
            "--no-sign-request",
            "--skip-lineage",
            # This folder contains two yaml one valid dataset yaml and another non dataset yaml
            "s3://odc-tools-test/derivative/ga_ls5t_nbart_gm_cyear_3/3-0-0/x08/y23/1994--P1Y/*.yaml",
            "ga_ls5t_nbart_gm_cyear_3",
            "--env",
            env_name,
        ],
    )
    assert result.exit_code == 1, f"Output: {result.output}"
    assert (
        result.output == "Added 1 datasets, skipped 0 datasets and failed 1 datasets.\n"
    )


def test_s3_to_dc_list_absolute_urls(
    mocked_s3_datasets, odc_test_db_with_products, env_name
) -> None:
    # provide mulitple uris, as absolute URLs
    runner = CliRunner()
    result = runner.invoke(
        s3_to_dc,
        [
            "--no-sign-request",
            "s3://odc-tools-test/cemp_insar/01/07/alos_cumul_2010-01-07.yaml",
            "s3://odc-tools-test/cemp_insar/04/01/alos_cumul_2010-04-01.yaml",
            "s3://odc-tools-test/cemp_insar/08/11/alos_cumul_2010-08-11.yaml",
            "cemp_insar_alos_displacement",
            "--env",
            env_name,
        ],
    )
    assert result.exit_code == 0, f"Output: {result.output}"
    assert (
        result.output == "Added 3 datasets, skipped 0 datasets and failed 0 datasets.\n"
    )


def test_s3_to_dc_no_product(
    mocked_s3_datasets, odc_test_db_with_products, env_name
) -> None:
    # product should not need to be specified
    runner = CliRunner()
    result = runner.invoke(
        s3_to_dc,
        [
            "--no-sign-request",
            "s3://odc-tools-test/cemp_insar/01/07/alos_cumul_2010-01-07.yaml",
            "--env",
            env_name,
        ],
        catch_exceptions=False,
    )
    assert result.exit_code == 0, f"Output: {result.output}"
    assert (
        result.output == "Added 1 datasets, skipped 0 datasets and failed 0 datasets.\n"
    )

    # test with glob
    result2 = CliRunner().invoke(
        s3_to_dc,
        [
            "--no-sign-request",
            "--stac",
            "s3://odc-tools-test/sentinel-s2-l2a-cogs/31/Q/GB/2020/8/S2B_31QGB_20200831_0_L2A/*_L2A.json",
            "--rename-product",
            "s2_l2a",
            "--env",
            env_name,
        ],
        catch_exceptions=False,
    )
    assert result2.exit_code == 0, f"Output: {result2.output}"
    assert (
        result2.output
        == "Added 1 datasets, skipped 0 datasets and failed 0 datasets.\n"
    )


def test_convert_bools(mocked_s3_datasets, odc_test_db_with_products, env_name) -> None:
    dc = odc_test_db_with_products
    assert dc.index.products.get_by_name("ga_s1_iw_hh_c1") is not None
    runner = CliRunner()
    result = runner.invoke(
        s3_to_dc,
        [
            "--no-sign-request",
            "--convert-bools",
            "--stac",
            "s3://odc-tools-test/experimental/linkage/s1_rtc_c1/t007_014549_iw1/2025/1/29/*.json",
            "--env",
            env_name,
        ],
        catch_exceptions=False,
    )
    assert result.exit_code == 0, f"Output: {result.output}"
    ds = dc.index.datasets.get("5f94dd81-241f-559a-9362-05b223d45ae1")
    # boolean values converted to strings
    assert ds.metadata.noise_removal_applied == "true"
    # other values left as they are
    assert ds.metadata.speckle_filter_applied == "False"
    assert ds.metadata_doc["properties"]["sarard:speckle_filter_window"] == []
