import pytest
from odc.algo._grouper import group_by_nothing, key2num, mid_longitude, solar_offset

from datacube.testutils import mk_sample_dataset
from datacube.utils import geometry as geom


@pytest.mark.parametrize("lon,lat", [(0, 10), (100, -10), (-120, 30)])
def test_mid_lon(lon, lat):
    r = 0.1
    rect = geom.box(lon - r, lat - r, lon + r, lat + r, "epsg:4326")
    assert rect.centroid.coords[0] == pytest.approx((lon, lat))

    assert mid_longitude(rect) == pytest.approx(lon)
    assert mid_longitude(rect.to_crs("epsg:3857")) == pytest.approx(lon)

    offset = solar_offset(rect, "h")
    assert offset.seconds % (60 * 60) == 0

    offset_sec = solar_offset(rect, "s")
    assert abs((offset - offset_sec).seconds) <= 60 * 60


@pytest.mark.parametrize(
    "input,expect",
    [
        ("ABAAC", [0, 1, 0, 0, 2]),
        ("B", [0]),
        ([1, 1, 1], [0, 0, 0]),
        ("ABCC", [0, 1, 2, 2]),
    ],
)
def test_key2num(input, expect):
    rr = list(key2num(input))
    assert rr == expect

    reverse = {}
    rr = list(key2num(input, reverse))
    assert rr == expect
    assert set(reverse.keys()) == set(range(len(set(input))))
    assert set(reverse.values()) == set(input)
    # first entry always gets an index of 0
    assert reverse[0] == input[0]


@pytest.fixture
def sample_geobox():
    yield geom.GeoBox.from_geopolygon(
        geom.box(-10, -20, 11, 22, "epsg:4326"), resolution=(-1, 1)
    )


@pytest.fixture
def sample_ds(sample_geobox):
    yield mk_sample_dataset([dict(name="red")], geobox=sample_geobox)


def test_grouper(sample_ds):
    xx = group_by_nothing([sample_ds])
    assert xx.values[0] == (sample_ds,)
    assert xx.uuid.values[0] == sample_ds.id

    xx = group_by_nothing([sample_ds, sample_ds], solar_offset(sample_ds.extent))
    assert xx.values[0] == (sample_ds,)
    assert xx.values[0] == (sample_ds,)
    assert xx.uuid.values[1] == sample_ds.id
    assert xx.uuid.values[1] == sample_ds.id
