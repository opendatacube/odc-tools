"""
Test for stac_transform
"""

from datacube.utils.changes import get_doc_changes
from odc.apps.dc_tools._stac import stac_transform, _geographic_to_projected


def test_geographic_to_projected(geometry_with_a_twist):
    transformed = _geographic_to_projected(geometry_with_a_twist, "EPSG:3832")

    assert transformed is not None

    expected = {
        "type": "Polygon",
        "coordinates": (
            (
                (3498004.815848052, -1894234.3033218135),
                (3341469.1401383593, -1858599.0018098454),
                (3337040.973246038, -1720470.5990706773),
                (3349096.8852309, -1669403.410330984),
                (3498004.815848052, -1894234.3033218135),
            ),
        ),
    }

    assert transformed.json == expected


def test_esri_lulc_stac_transform(esri_lulc_stac):
    transformed_stac_doc = stac_transform(esri_lulc_stac)
    expected_geometry_coordinates = (
        (
            (661287.0, 6209183.0),
            (614583.0, 6232873.0),
            (613410.0, 6233822.0),
            (612362.0, 6235174.0),
            (578054.0, 6285693.0),
            (577326.0, 6286947.0),
            (516925.0, 6401201.0),
            (515881.0, 6403393.0),
            (515178.0, 6406016.0),
            (514941.0, 6407721.0),
            (514850.0, 6409474.0),
            (514909.0, 6411230.0),
            (563156.0, 6887656.0),
            (563528.0, 6890381.0),
            (563901.0, 6891859.0),
            (564375.0, 6893191.0),
            (565363.0, 6895006.0),
            (566609.0, 6896326.0),
            (590299.0, 6919805.0),
            (591572.0, 6920477.0),
            (612179.0, 6928653.0),
            (624494.0, 6931468.0),
            (626035.0, 6931619.0),
            (627092.0, 6931319.0),
            (639943.0, 6927586.0),
            (643145.0, 6926090.0),
            (643924.0, 6925405.0),
            (644712.0, 6924404.0),
            (645547.0, 6922879.0),
            (646233.0, 6921043.0),
            (646681.0, 6919325.0),
            (647084.0, 6916926.0),
            (671635.0, 6559162.0),
            (671926.0, 6552687.0),
            (678860.0, 6397314.0),
            (687071.0, 6210141.0),
            (661287.0, 6209183.0),
        ),
    )

    assert (
        transformed_stac_doc["geometry"]["coordinates"] == expected_geometry_coordinates
    )


def test_landsat_stac_transform(landsat_stac, landsat_odc):
    transformed_stac_doc = stac_transform(landsat_stac)
    assert len(get_doc_changes(transformed_stac_doc, landsat_odc)) == 5


def test_sentinel_stac_transform(sentinel_stac_old, sentinel_odc):
    transformed_stac_doc = stac_transform(sentinel_stac_old)
    doc_changes = get_doc_changes(transformed_stac_doc, sentinel_odc)
    assert len(doc_changes) == 1


def test_sentinel_c1_stac_transform(sentinel_c1_stac, sentinel_c1_odc):
    transformed_stac_doc = stac_transform(sentinel_c1_stac)
    doc_changes = get_doc_changes(transformed_stac_doc, sentinel_c1_odc)
    assert len(doc_changes) == 0
    # test that absolute links are maintained for accessories
    assert (
        transformed_stac_doc["accessories"]["thumbnail"]["path"]
        == sentinel_c1_stac["assets"]["thumbnail"]["href"]
    )
    assert (
        transformed_stac_doc["accessories"]["product_metadata"]["path"]
        == sentinel_c1_stac["assets"]["product_metadata"]["href"]
    )
    assert (
        transformed_stac_doc["accessories"]["tileinfo_metadata"]["path"]
        == sentinel_c1_stac["assets"]["tileinfo_metadata"]["href"]
    )
    assert (
        transformed_stac_doc["accessories"]["granule_metadata"]["path"]
        == sentinel_c1_stac["assets"]["granule_metadata"]["href"]
    )


def test_sentinel_c1_rel_stac_transform(sentinel_c1_rel_stac, sentinel_c1_rel_odc):
    transformed_stac_doc = stac_transform(sentinel_c1_rel_stac)
    doc_changes = get_doc_changes(transformed_stac_doc, sentinel_c1_rel_odc)
    assert len(doc_changes) == 0
    # test that absolute links are converted to relative links for accessories
    assert transformed_stac_doc["accessories"]["thumbnail"]["path"] == "L2A_PVI.jpg"
    assert (
        transformed_stac_doc["accessories"]["product_metadata"]["path"]
        == "product_metadata.xml"
    )
    assert (
        transformed_stac_doc["accessories"]["tileinfo_metadata"]["path"]
        == "tileInfo.json"
    )
    assert (
        transformed_stac_doc["accessories"]["granule_metadata"]["path"]
        == "metadata.xml"
    )


def test_usgs_landsat_stac_transform(usgs_landsat_stac):
    transformed_stac_doc = stac_transform(usgs_landsat_stac)

    expected_geometry_coordinates = (
        (
            (1087485.0, -313215.0),
            (904635.0, -356445.0),
            (860745.0, -172485.0),
            (1043085.0, -128325.0),
            (1087485.0, -313215.0),
        ),
    )
    assert (
        transformed_stac_doc["geometry"]["coordinates"] == expected_geometry_coordinates
    )


def test_lidar_stac_transform(lidar_stac):
    transformed_stac_doc = stac_transform(lidar_stac)
    expected_geometry_coordinates = (
        (
            (766000.0, 6732000.0),
            (766000.0, 6730000.0),
            (768000.0, 6730000.0),
            (768000.0, 6732000.0),
            (766000.0, 6732000.0),
        ),
    )
    assert (
        transformed_stac_doc["geometry"]["coordinates"] == expected_geometry_coordinates
    )


def test_world_wrapping_stac(world_wrapping_stac):
    transformed_stac_doc = stac_transform(world_wrapping_stac)
    expected_geometry_coordinates = (
        (3336000.0, -2079990.0),
        (3336000.0, -1983990.0),
        (3432030.0, -1983990.0),
        (3432030.0, -2079990.0),
        (3336000.0, -2079990.0),
    )
    assert (
        transformed_stac_doc["geometry"]["coordinates"][0]
        == expected_geometry_coordinates
    )
