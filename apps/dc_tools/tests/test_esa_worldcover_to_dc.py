import pytest
from click.testing import CliRunner

from odc.apps.dc_tools.esa_worldcover_to_dc import _unpack_bbox, cli, get_tile_uris


@pytest.fixture
def bbox():
    return "5,5,7,7"


@pytest.fixture
def bbox_africa():
    return "-26.359944882003788,-47.96476498374171,64.4936701740102,38.34459242512347"


def test_bboxes():
    bbox = "0,0,1,1"
    bounding_box = [float(x) for x in bbox.split(",")]
    assert _unpack_bbox(bounding_box) == (0, 0, 3, 3)
    assert len(list(get_tile_uris(bbox))) == 1

    bbox = "0,0,3,3"
    bounding_box = [float(x) for x in bbox.split(",")]
    assert _unpack_bbox(bounding_box) == (0, 0, 3, 3)
    assert len(list(get_tile_uris(bbox))) == 1

    bbox = "1,1,5,5"
    bounding_box = [float(x) for x in bbox.split(",")]
    assert _unpack_bbox(bounding_box) == (0, 0, 6, 6)

    bbox = "-98,15,-90,30"
    bounding_box = [float(x) for x in bbox.split(",")]
    assert _unpack_bbox(bounding_box) == (-99, 15, -90, 30)
    assert len(list(get_tile_uris(bbox))) == 15


def test_get_dem_tile_uris(bbox):
    uris = list(get_tile_uris(bbox))

    assert uris[0][0] == (
        "https://esa-worldcover.s3.eu-central-1.amazonaws.com/"
        "v100/2020/map/ESA_WorldCover_10m_2020_v100_N03E003_Map.tif"
    )

    assert len(uris) == 4


def test_complex_bbox(bbox_africa):
    uris = list(get_tile_uris(bbox_africa))

    assert len(uris) == 899


# Test the actual process
def test_indexing_cli(bbox, odc_test_db_with_products):
    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "--bbox",
            bbox,
            "--statsd-setting",
            "localhost:8125",
        ],
    )
    assert result.exit_code == 0
