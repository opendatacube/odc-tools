import pytest

from click.testing import CliRunner

from odc.apps.dc_tools.esri_land_cover_to_dc import get_item, cli
from odc.apps.dc_tools.utils import get_esri_list


@pytest.fixture
def file_list():
    return list(get_esri_list())


def test_file_list(file_list):
    assert len(file_list) == 728

    assert file_list[0][-1] != "\n"


def test_one_transformed_item(file_list):
    _ = [x for x in get_item(file_list[0])]


# Test the actual process
def test_indexing_cli():
    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "--add-product",
            "--limit",
            "10"
        ],
    )
    assert result.exit_code == 0
