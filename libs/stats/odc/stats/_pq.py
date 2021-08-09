"""
Sentinel 2 pixel quality stats
"""
from functools import partial
from typing import List, Dict, Optional, Tuple, cast, Iterable

import xarray as xr

from odc.algo import enum_to_bool, mask_cleanup
from odc.algo._masking import _or_fuser
from odc.algo.io import load_with_native_transform
from odc.stats.model import Task

from .model import StatsPluginInterface
from . import _plugins

cloud_classes = (
    "cloud shadows",
    "cloud medium probability",
    "cloud high probability",
    "thin cirrus",
)

# filters = list of iterable tuples of morphological operations in the order you want them performed,
# e.g. [[("opening", 2), ("dilation", 5)]]
default_filters = [[("opening", 2), ("dilation", 5)], [("opening", 0), ("dilation", 5)]]

class StatsPQ(StatsPluginInterface):
    NAME = "pc_s2_annual"
    SHORT_NAME = NAME
    VERSION = '0.0.1'
    PRODUCT_FAMILY = "pixel_quality_statistics"

    def __init__(
        self,
        filters: Optional[List[Iterable[Tuple[str, int]]]] = None,
        resampling: str = "nearest",
    ):
        if filters is None:
            filters = default_filters
        self.filters = filters
        self.resampling = resampling

    @property
    def measurements(self) -> Tuple[str, ...]:
        measurements = ["total", "clear"]

        for mask_filters in self.filters or []:
            clear_filter_band_name = "clear"
            for operation, radius in mask_filters:
                clear_filter_band_name += f"_{radius:d}"
            measurements.append(clear_filter_band_name)

        return tuple(measurements)

    def input_data(self, task: Task) -> xr.Dataset:
        """
        .valid           Bool
        .erased          Bool
        .erased{postfix} Bool
        """
        filters = self.filters
        resampling = self.resampling

        return load_with_native_transform(
            task.datasets,
            ["SCL"],
            task.geobox,
            _pq_native_transform,
            groupby="solar_day",
            resampling=resampling,
            fuser=partial(_pq_fuser, filters=filters),
            chunks={"x": -1, "y": -1},
        )

    def reduce(self, xx: xr.Dataset) -> xr.Dataset:
        """
        .valid            -> .total
        .erased           -> .clear
        .erased{postfix}  -> .clear{postfix} For All {postfix}
        """
        valid = xx.valid
        erased_bands = [str(n) for n in xx.data_vars if str(n).startswith("erased")]
        total = valid.sum(axis=0, dtype="uint16")
        pq = xr.Dataset(dict(total=total))

        for band in erased_bands:
            erased: xr.DataArray = cast(xr.DataArray, xx[band])
            clear_name = band.replace("erased", "clear")
            clear = valid * (~erased)  # type: ignore
            pq[clear_name] = clear.sum(axis=0, dtype="uint16")

        return pq


def _pq_native_transform(xx: xr.Dataset) -> xr.Dataset:
    """
    config:
    cloud_classes

    .SCL -> .valid   (anything but nodata)
            .erased  (cloud pixels only)
    """
    scl = xx.SCL
    valid = scl != scl.nodata
    erased = enum_to_bool(scl, cloud_classes)
    return xr.Dataset(
        dict(valid=valid, erased=erased),
        attrs={"native": True},  # <- native flag needed for fuser
    )


def _pq_fuser(
    xx: xr.Dataset,
    filters: Optional[List[Iterable[Tuple[str, int]]]] = None
) -> xr.Dataset:
    """
    Native:
      .valid    -> .valid  (fuse with OR)
      .erased   -> .erased (fuse with OR)
                -> .erased{postfix} for All Filters (mask_cleanup applied post-fusing)
    Projected:
     fuse all bands with OR

    """
    is_native = xx.attrs.get("native", False)
    xx = xx.map(_or_fuser)
    xx.attrs.pop("native", None)

    if is_native:
        for mask_filters in filters or []:
            erased_filter_band_name = "erased"
            for operation, radius in mask_filters:
                erased_filter_band_name += f"_{radius:d}"
            xx[erased_filter_band_name] = mask_cleanup(xx["erased"], mask_filters=mask_filters)

    return xx


_plugins.register("pq", StatsPQ)


def test_pq_product():
    location = "file:///tmp"
    product = StatsPQ().product(location)
    assert product.measurements == ("total", "clear", "clear_2_5", "clear_0_5")

    product = StatsPQ(filters=[]).product(location)
    assert product.measurements == ("total", "clear")


def test_plugin():
    location = "file:///tmp"
    pq = _plugins.resolve("pq")()
    product = pq.product(location)

    assert product.measurements == ("total", "clear", "clear_2_5", "clear_0_5")
    assert pq.filters == default_filters

    pq = _plugins.resolve("pq")(filters=[], resampling="cubic")
    product = pq.product(location)

    assert product.measurements == ("total", "clear")
    assert pq.filters == []
    assert pq.resampling == "cubic"
