import pytest
from click.testing import CliRunner
from odc.apps.dc_tools.fs_to_dc import cli, _find_files

from pathlib import Path

TEST_DATA_FOLDER: Path = Path(__file__).parent.joinpath("data")


def test_find_yamls(test_data_dir):
    # Default is to find YAML files
    files = [str(x) for x in _find_files(test_data_dir)]

    assert len(files) == 2
    assert str(
        Path(test_data_dir)
        / "ga_ls8c_ard_3-1-0_088080_2020-05-25_final.odc-metadata.sqs.yaml"
    ) in files


def test_find_json(test_data_dir):
    # Search for JSON files
    files = [str(x) for x in _find_files(test_data_dir, stac=True)]

    assert len(files) == 9
    assert (
        str(
            Path(test_data_dir)
            / "ga_ls8c_ard_3-1-0_088080_2020-05-25_final.stac-item.json"
        )
        in files
    )


def test_find_glob(test_data_dir):
    # Find anything
    files = [x for x in _find_files(test_data_dir, glob="**/*.*")]

    assert len(files) >= 6


@pytest.mark.depends(on=["add_products"])
def test_fs_to_fc_yaml(test_data_dir):
    runner = CliRunner()
    result = runner.invoke(
        cli,
        [test_data_dir, "--stac", "--glob=**/NASADEM_HGT_s56w072.stac-item.json"],
    )
    assert result.exit_code == 0


@pytest.fixture
def test_data_dir():
    return str(TEST_DATA_FOLDER)
