import pytest

from click.testing import CliRunner

from odc.apps.dc_tools.cop_dem_to_dc import get_dem_tile_uris, cli


PRODUCTS = ["cop_30", "cop_90"]


@pytest.fixture
def bbox():
    return '5,5,7,7'


@pytest.mark.parametrize("product", PRODUCTS)
def test_get_dem_tile_uris(bbox, product):
    uris = list(get_dem_tile_uris(bbox, product))

    if product == "cop_30":
        assert uris[0][0] == (
            "s3://copernicus-dem-30m/Copernicus_DSM_COG_10_N05_00_E005_00_DEM/"
            "Copernicus_DSM_COG_10_N05_00_E005_00_DEM.tif"
        )
    else:
        assert uris[0][0] == (
            "s3://copernicus-dem-90m/Copernicus_DSM_COG_30_N05_00_E005_00_DEM/"
            "Copernicus_DSM_COG_30_N05_00_E005_00_DEM.tif"
        )

    assert len(uris) == 4


# Test the actual process
@pytest.mark.depends(on='have_db')
@pytest.mark.depends(on=['add_products'])
@pytest.mark.parametrize("product", PRODUCTS)
def test_indexing_cli(bbox, product):
    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "--bbox",
            bbox,
            "--product",
            product
        ],
    )
    assert result.exit_code == 0

    result = runner.invoke(
        cli,
        [
            "--bbox",
            bbox,
            "--product",
            "cop_90"
        ],
    )
    assert result.exit_code == 0
