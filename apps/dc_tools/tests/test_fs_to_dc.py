from click.testing import CliRunner
from pathlib import Path

from odc.apps.dc_tools.fs_to_dc import cli as fs_to_dc_cli

TEST_DATA_FOLDER: Path = Path(__file__).parent.joinpath("data")


def test_fs_to_fc_yaml(test_data_dir, odc_test_db_with_products):
    runner = CliRunner()
    result = runner.invoke(
        fs_to_dc_cli,
        [
            test_data_dir,
            "--stac",
            "--glob=**/NASADEM_HGT_s56w072.stac-item.json",
        ],
        catch_exceptions=False,
    )
    assert result.exit_code == 0


def test_archive_less_mature(odc_db, test_data_dir, nrt_dsid, final_dsid):
    dc = odc_db
    runner = CliRunner()

    # Index NRT dataset
    result = runner.invoke(
        fs_to_dc_cli,
        [
            test_data_dir,
            "--glob=**/maturity-nrt.odc-metadata.yaml",
            "--archive-less-mature",
        ],
    )
    assert result.exit_code == 0
    assert dc.index.datasets.get(final_dsid) is None
    assert dc.index.datasets.get(nrt_dsid).archived_time is None

    # Index Final dataset (autoarchiving NRT)
    result = runner.invoke(
        fs_to_dc_cli,
        [
            test_data_dir,
            "--glob=**/maturity-final.odc-metadata.yaml",
            "--archive-less-mature",
        ],
    )
    assert result.exit_code == 0
    assert dc.index.datasets.get(final_dsid).archived_time is None
    assert dc.index.datasets.get(nrt_dsid).archived_time is not None


def test_dont_archive_less_mature(odc_db, test_data_dir, nrt_dsid, final_dsid):
    # no archiving should be done if --archive-less-mature is not set
    dc = odc_db
    runner = CliRunner()

    # Index NRT dataset
    result = runner.invoke(
        fs_to_dc_cli,
        [
            test_data_dir,
            "--glob=**/maturity-nrt.odc-metadata.yaml",
        ],
    )
    assert result.exit_code == 0
    assert dc.index.datasets.get(final_dsid) is None
    assert dc.index.datasets.get(nrt_dsid).archived_time is None

    # Index Final dataset (autoarchiving NRT)
    result = runner.invoke(
        fs_to_dc_cli,
        [
            test_data_dir,
            "--glob=**/maturity-final.odc-metadata.yaml",
        ],
    )
    assert result.exit_code == 0
    assert dc.index.datasets.get(final_dsid).archived_time is None
    assert dc.index.datasets.get(nrt_dsid).archived_time is None


def test_keep_more_mature(odc_db, test_data_dir, nrt_dsid, final_dsid):
    dc = odc_db
    runner = CliRunner()

    # Index Final dataset
    result = runner.invoke(
        fs_to_dc_cli,
        [
            test_data_dir,
            "--glob=**/maturity-final.odc-metadata.yaml",
            "--archive-less-mature",
        ],
    )
    assert result.exit_code == 0
    assert dc.index.datasets.get(nrt_dsid) is None
    assert dc.index.datasets.get(final_dsid).archived_time is None

    # Index NRT dataset (less mature - should be skipped)
    result = runner.invoke(
        fs_to_dc_cli,
        [
            test_data_dir,
            "--glob=**/maturity-nrt.odc-metadata.yaml",
            "--archive-less-mature",
        ],
    )
    assert result.exit_code == 0
    assert dc.index.datasets.get(final_dsid).archived_time is None
    assert dc.index.datasets.get(nrt_dsid) is None
