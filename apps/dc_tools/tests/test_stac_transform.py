"""
Test for stac_transform
"""
import pytest

from odc.apps.dc_tools._stac import stac_transform
from datacube.utils.changes import get_doc_changes


def test_landsat_stac_transform(landsat_stac, landsat_odc):
    transformed_stac_doc = stac_transform(landsat_stac)
    assert len(get_doc_changes(transformed_stac_doc, landsat_odc)) == 6


def test_sentinel_stac_transform(sentinel_stac, sentinel_odc):
    transformed_stac_doc = stac_transform(sentinel_stac)
    assert len(get_doc_changes(transformed_stac_doc, sentinel_odc)) == 2


@pytest.mark.skip(reason="Incomplete test")
def test_usgs_landsat_stac_transform(usgs_landsat_stac):
    _ = stac_transform(usgs_landsat_stac)


@pytest.mark.skip(reason="Incomplete test")
def test_lidar_stac_transform(lidar_stac):
    _ = stac_transform(lidar_stac)