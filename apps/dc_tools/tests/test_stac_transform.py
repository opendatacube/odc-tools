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


def test_usgs_landsat_stac_transform(usgs_landsat_stac):
    transformed_stac_doc = stac_transform(usgs_landsat_stac)
    assert transformed_stac_doc['geometry']['coordinates'] == (((1087485.0, -313215.0), (904635.0, -356445.0), (860745.0, -172485.0), (1043085.0, -128325.0), (1087485.0, -313215.0)), )


def test_lidar_stac_transform(lidar_stac):
    transformed_stac_doc = stac_transform(lidar_stac)
    assert transformed_stac_doc['geometry']['coordinates'] == (((765999.0, 6731999.0), (765999.0, 6729999.0), (767999.0, 6729999.0), (767999.0, 6731999.0), (765999.0, 6731999.0)),)