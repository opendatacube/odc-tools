"""
Test for stac_transform
"""
import pytest

from datacube.utils.changes import get_doc_changes
from odc.apps.dc_tools._stac import stac_transform


def test_esri_lulc_stac_transform(esri_lulc_stac):
    transformed_stac_doc = stac_transform(esri_lulc_stac)
    expected_geometry_coordinates = [
        [
            pytest.approx(coords, 1)
            for coords in [
                [1087485.0, -313215.0],
                [904635.0, -356445.0],
                [860745.0, -172485.0],
                [1043085.0, -128325.0],
                [1087485.0, -313215.0],
            ]
        ]
    ]
    assert (
        transformed_stac_doc["geometry"]["coordinates"] == expected_geometry_coordinates
    )


def test_landsat_stac_transform(landsat_stac, landsat_odc):
    transformed_stac_doc = stac_transform(landsat_stac)
    assert len(get_doc_changes(transformed_stac_doc, landsat_odc)) == 5


def test_sentinel_stac_transform(sentinel_stac_old, sentinel_odc):
    transformed_stac_doc = stac_transform(sentinel_stac_old)
    assert len(get_doc_changes(transformed_stac_doc, sentinel_odc)) == 1


def test_usgs_landsat_stac_transform(usgs_landsat_stac):
    transformed_stac_doc = stac_transform(usgs_landsat_stac)
    expected_geometry_coordinates = [
        [
            pytest.approx(coords, 1)
            for coords in [
                [1087485.0, -313215.0],
                [904635.0, -356445.0],
                [860745.0, -172485.0],
                [1043085.0, -128325.0],
                [1087485.0, -313215.0],
            ]
        ]
    ]
    assert (
        transformed_stac_doc["geometry"]["coordinates"] == expected_geometry_coordinates
    )


def test_lidar_stac_transform(lidar_stac):
    transformed_stac_doc = stac_transform(lidar_stac)
    expected_geometry_coordinates = [
        [
            pytest.approx(coords, 1)
            for coords in [
                [765999.0, 6731999.0],
                [765999.0, 6729999.0],
                [767999.0, 6729999.0],
                [767999.0, 6731999.0],
                [765999.0, 6731999.0],
            ]
        ]
    ]
    assert (
        transformed_stac_doc["geometry"]["coordinates"] == expected_geometry_coordinates
    )
