import pytest
from odc.algo.io import choose_transform_path


@pytest.mark.xfail(reason="Fragile test code, I think between different GDAL versions")
@pytest.mark.parametrize("transform_code", [None, "EPSG:9688", "EPSG:1150"])
@pytest.mark.parametrize("area_of_interest", [None, [-180, -90, 180, 90]])
def test_choose_transform_path(transform_code, area_of_interest):
    src_crs = "EPSG:32649"
    dst_crs = "EPSG:3577"
    proj_str = {
        "9688": "+proj=pipeline +step +inv +proj=utm +zone=49 +ellps=WGS84 "
        "+step +proj=push +v_3 "
        "+step +proj=cart +ellps=WGS84 "
        "+step +inv +proj=helmert +x=0.06155 +y=-0.01087 +z=-0.04019 "
        "+rx=-0.0394924 +ry=-0.0327221 +rz=-0.0328979 +s=-0.009994 "
        "+convention=coordinate_frame "
        "+step +inv +proj=cart +ellps=GRS80 "
        "+step +proj=pop +v_3 "
        "+step +proj=aea +lat_0=0 +lon_0=132 +lat_1=-18 +lat_2=-36 "
        "+x_0=0 +y_0=0 +ellps=GRS80",
        "1150": "+proj=pipeline +step +inv +proj=utm +zone=49 +ellps=WGS84 "
        "+step +proj=aea +lat_0=0 +lon_0=132 +lat_1=-18 +lat_2=-36 "
        "+x_0=0 +y_0=0 +ellps=GRS80",
    }
    if transform_code is None and area_of_interest is None:
        assert (
            choose_transform_path(src_crs, dst_crs, transform_code, area_of_interest)
            == {}
        )
    elif area_of_interest is None:
        with pytest.raises(ValueError):
            choose_transform_path(src_crs, dst_crs, transform_code, area_of_interest)
    elif transform_code is None:
        assert choose_transform_path(
            src_crs, dst_crs, transform_code, area_of_interest
        ) == {"COORDINATE_OPERATION": proj_str.get("9688")}
    else:
        assert choose_transform_path(
            src_crs, dst_crs, transform_code, area_of_interest
        ) == {"COORDINATE_OPERATION": proj_str.get(transform_code.split(":")[1], "")}
