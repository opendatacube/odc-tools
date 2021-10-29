"""
Sentinel 2 pixel quality stats
"""
from typing import Dict, Iterable, Optional, Tuple, cast

import xarray as xr
from odc.algo import enum_to_bool, mask_cleanup
from odc.algo._masking import _or_fuser

from ._registry import StatsPluginInterface, register

cloud_classes = (
    "cloud shadows",
    "cloud medium probability",
    "cloud high probability",
    "thin cirrus",
)

# filters - dict of band name and list of iterable tuples of morphological operations
#           in the order you want them to perform
default_filters = {
  "clear_2_5": [("opening", 2), ("dilation", 5)],
  "clear_0_5": [("opening", 0), ("dilation", 5)]
}


class StatsPQ(StatsPluginInterface):
    NAME = "pc_s2_annual"
    SHORT_NAME = NAME
    VERSION = '0.0.1'
    PRODUCT_FAMILY = "pixel_quality_statistics"

    def __init__(
        self,
        filters: Optional[Dict[str, Iterable[Tuple[str, int]]]] = None,
        resampling: str = "nearest",
        **kwargs
    ):
        super().__init__(input_bands=["SCL"], resampling=resampling, **kwargs)
        if filters is None:
            filters = default_filters
        self.filters = filters

    @property
    def measurements(self) -> Tuple[str, ...]:
        measurements = [
            "total",
            "clear",
            *list(self.filters)
        ]

        return tuple(measurements)

    def fuser(self, xx: xr.Dataset) -> xr.Dataset:
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
            for band, mask_filters in self.filters.items():
                erased_filter_band_name = band.replace("clear", "erased")
                xx[erased_filter_band_name] = mask_cleanup(xx["erased"], mask_filters=mask_filters)

        return xx

    def native_transform(self, xx: xr.Dataset) -> xr.Dataset:
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


register("pq", StatsPQ)
