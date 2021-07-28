from pathlib import Path

import pytest
from click.testing import CliRunner
from odc.apps.dc_tools.add_update_products import _get_product, _parse_csv, cli

TEST_DATA_FOLDER: Path = Path(__file__).parent.joinpath("data")
LOCAL_EXAMPLE: str = "example_product_list.csv"
REMOTE_EXAMPLE: str = (
    "https://raw.githubusercontent.com/GeoscienceAustralia/"
    "dea-config/master/workspaces/dev-products.csv"
)
PRODUCT_EXAMPLE: str = (
    "https://raw.githubusercontent.com/digitalearthafrica/"
    "config/master/products/esa_s2_l2a.odc-product.yaml"
)


def test_parse_local_csv(local_csv):
    local_contents = [x for x in _parse_csv(local_csv)]

    assert len(local_contents) == 5
    assert local_contents[0].name == "s2_l2a"


@pytest.mark.xfail(reason="Test is failing because the CSV is non-conforming")
def test_parse_remote_csv(remote_csv):
    remote_contents = [x for x in _parse_csv(remote_csv)]
    assert len(remote_contents) >= 65


def test_load_product_def(remote_product):
    products = _get_product(remote_product)

    assert products[0]["name"] == "s2_l2a"


@pytest.mark.depends(name='add_products')
def test_add_products(local_csv):
    runner = CliRunner()
    # This will fail if requester pays is enabled
    result = runner.invoke(
        cli,
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
def remote_csv():
    return REMOTE_EXAMPLE
