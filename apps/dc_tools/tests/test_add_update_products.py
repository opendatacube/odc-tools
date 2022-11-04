from pathlib import Path

import pytest
from click.testing import CliRunner
from datacube import Datacube
from odc.apps.dc_tools.add_update_products import _get_product, _parse_csv
from odc.apps.dc_tools.add_update_products import cli as add_update_products_cli

TEST_DATA_FOLDER: Path = Path(__file__).parent.joinpath("data")
LOCAL_EXAMPLE: str = "example_product_list.csv"
PRODUCT_EXAMPLE: str = (
    "https://raw.githubusercontent.com/digitalearthafrica/"
    "config/master/products/esa_s2_l2a.odc-product.yaml"
)


def test_parse_local_csv(local_csv):
    local_contents = [x for x in _parse_csv(local_csv)]

    assert len(local_contents) == 11
    assert local_contents[0].name == "s2_l2a"


def test_parse_remote_csv(remote_csv):
    remote_contents = [x for x in _parse_csv(remote_csv)]
    assert len(remote_contents) == 11
    assert remote_contents[0].name == "s2_l2a"


def test_load_product_def(remote_product):
    products = _get_product(remote_product)

    assert products[0]["name"] == "s2_l2a"


def test_add_products(local_csv, odc_db):
    runner = CliRunner()
    # This will fail if requester pays is enabled
    result = runner.invoke(
        add_update_products_cli,
        [
            local_csv,
            "--update-if-exists",
        ],
    )
    assert result.exit_code == 0


@pytest.fixture
def remote_product():
    return PRODUCT_EXAMPLE


@pytest.fixture
def local_csv():
    return str(TEST_DATA_FOLDER / LOCAL_EXAMPLE)


@pytest.fixture
def remote_csv(httpserver, local_csv):
    httpserver.expect_request("/some.csv").respond_with_data(open(local_csv).read())
    yield httpserver.url_for("/some.csv")
