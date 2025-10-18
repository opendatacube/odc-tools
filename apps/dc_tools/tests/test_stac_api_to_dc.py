# Tests for the stac_api-to-dc CLI tool
from click.testing import CliRunner
from odc.apps.dc_tools.stac_api_to_dc import cli, item_to_meta_uri
from odc.apps.dc_tools.utils import MICROSOFT_PC_STAC_URI
from pystac import Item


def test_rewrite_urls(landsat_stac, odc_test_db_with_products):
    url_rewrite_tuple = (
        "https://dea-public-data-dev.s3-ap-southeast-2.amazonaws.com",
        "s3://dea-public-data-dev",
    )

    item = Item.from_dict(landsat_stac)

    print(item.self_href)

    _, uri, _ = item_to_meta_uri(
        item,
        odc_test_db_with_products,
        rename_product="ls8_c2l2_sr",
        url_string_replace=url_rewrite_tuple,
    )

    # https://dea-public-data-dev.s3-ap-southeast-2.amazonaws.com/
    # analysis-ready-data/ga_ls8c_ard_3/088/080/2020/05/25/ga_ls8c_ard_3-1-0_088080_2020-05-25_final.stac-item.json
    changed_uri = (
        "s3://dea-public-data-dev/analysis-ready-data/ga_ls8c_ard_3/088/080/2020/05/25/"
        "ga_ls8c_ard_3-1-0_088080_2020-05-25_final.stac-item.json"
    )
    assert uri == changed_uri


def test_stac_to_dc_earthsearch(odc_test_db_with_products, env_name) -> None:
    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "--catalog-href=https://earth-search.aws.element84.com/v1/",
            "--bbox=5,15,10,20",
            "--limit=10",
            "--collections=sentinel-2-l2a",  # sentinel-s2-l2a-cogs is no longer available
            "--rename-product=s2_l2a",
            "--datetime=2020-08-01/2020-08-31",
            "--env",
            env_name,
        ],
        catch_exceptions=False,
    )
    assert result.exit_code == 0, f"Output: {result.output}"
    assert "Added 10 Datasets, failed 0 Datasets, skipped 0 Datasets" in result.output


def test_stac_to_dc_usgs(odc_test_db_with_products, env_name) -> None:
    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "--catalog-href=https://landsatlook.usgs.gov/stac-server/",
            "--bbox=5,15,10,20",
            "--limit=10",
            "--collections=landsat-c2l2-sr",
            "--datetime=2020-08-01/2020-08-31",
            "--env",
            env_name,
        ],
        catch_exceptions=False,
    )
    assert result.exit_code == 0, f"Output: {result.output}"


def test_stac_to_dc_planetarycomputer(odc_test_db_with_products, env_name) -> None:
    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            f"--catalog-href={MICROSOFT_PC_STAC_URI}",
            "--limit=1",
            "--collections=nasadem",
            "--env",
            env_name,
        ],
    )
    assert result.exit_code == 0, f"Output: {result.output}"
