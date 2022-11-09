import pytest
from click.testing import CliRunner
from odc.apps.dc_tools.cop_dem_to_dc import cli, get_dem_tile_uris

PRODUCTS = ["cop_30", "cop_90"]


@pytest.fixture
def bbox():
    return "5,5,7,7"


@pytest.fixture
def bbox_africa():
    return "-26.359944882003788,-47.96476498374171,64.4936701740102,38.34459242512347"


@pytest.mark.parametrize("product", PRODUCTS)
def test_get_dem_tile_uris(bbox, product):
    uris = list(get_dem_tile_uris(bbox, product))

    if product == "cop_30":
        assert uris[0][0] == (
            "https://copernicus-dem-30m.s3.eu-central-1.amazonaws.com/"
            "Copernicus_DSM_COG_10_N05_00_E005_00_DEM/Copernicus_DSM_COG_10_N05_00_E005_00_DEM.tif"
        )
    else:
        assert uris[0][0] == (
            "https://copernicus-dem-90m.s3.eu-central-1.amazonaws.com/"
            "Copernicus_DSM_COG_30_N05_00_E005_00_DEM/Copernicus_DSM_COG_30_N05_00_E005_00_DEM.tif"
        )

    assert len(uris) == 4


def test_complex_bbox(bbox_africa):
    uris = list(get_dem_tile_uris(bbox_africa, "cop_30"))

    assert len(uris) == 8004


# Test the actual process
@pytest.mark.parametrize("product", PRODUCTS)
def test_indexing_cli(bbox, product):
    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "--add-product",
            "--statsd-setting",
            "localhost:8125",
            "--bbox",
            bbox,
            "--product",
            product,
        ],
    )
    assert result.exit_code == 0
    assert f"Product definition added for {product}" in result.output
    assert "Added 4 Datasets, failed 0 Datasets, skipped 0 Datasets" in result.output


@pytest.mark.parametrize("product", PRODUCTS)
def test_indexing_cli_repeat(bbox, product):
    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "--add-product",
            "--statsd-setting",
            "localhost:8125",
            "--bbox",
            bbox,
            "--product",
            product,
        ],
    )
    assert result.exit_code == 0
    assert f"Product definition added for {product}" in result.output
    assert "Added 0 Datasets, failed 0 Datasets, skipped 4 Datasets" in result.output
