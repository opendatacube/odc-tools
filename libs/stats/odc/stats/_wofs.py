"""
Wofs Summary
"""
from typing import Optional, Tuple
import xarray as xr
from odc.stats.model import Task
from odc.algo.io import load_with_native_transform
from odc.algo import safe_div, apply_numexpr, keep_good_only
from .model import StatsPluginInterface
from . import _plugins


class StatsWofs(StatsPluginInterface):
    NAME = "ga_ls_wo_summary"
    SHORT_NAME = NAME
    VERSION = "1.6.0"
    PRODUCT_FAMILY = "wo_summary"

    def __init__(
        self, resampling: str = "bilinear",
    ):
        self.resampling = resampling

    @property
    def measurements(self) -> Tuple[str, ...]:
        return ("count_wet", "count_clear", "frequency")

    def _native_tr(self, xx):
        """
        xx.water -- uint8 classifier bitmask

        .. code-block::

          |128| 64| 32| 16| |  8|  4|  2|  1|
          |---|---|---|---|=|---|---|---|---|
            7   6   5   4     3   2   1   0
            |   |   |   |     |   |   |   |
            |   |   |   |     |   |   |   x---> NODATA: 1 -- all bands were nodata
            |   |   |   |     |   |   o-------> Non Contiguous - some bands were nodata)
            |   |   |   |     |   x-----------> Low Solar Angle
            |   |   |   |     o---------------> Terrain Shadow
            |   |   |   |
            |   |   |   x---------------------> Terrain High Slope
            |   |   o-------------------------> Cloud Shadow
            |   x-----------------------------> Cloud
            o---------------------------------> Water

        """
        wet = xx.water == 128
        dry = xx.water == 0
        # bad -- if any bit in [1, 6] range is non zero
        #        X--- ---X
        #        7654 3210
        # Erase bits 7 and 0 and check if what's left is non-zero
        bad = apply_numexpr("((water%128)>>1) > 0", xx, dtype="bool")
        bad.attrs.pop("nodata", None)

        # some -- nodata flag is not set and contiguity flag is not set
        some = apply_numexpr("((water)%4) == 0", xx, dtype="bool")
        some.attrs.pop("nodata", None)
        return xr.Dataset(dict(wet=wet, dry=dry, bad=bad, some=some))

    def _fuser(self, xx):
        """
        xx.bad  -- don't count
        xx.wet  -- is wet
        xx.dry  -- is dry
        xx.some -- there was at least one non-nodata observation at that pixel
        """
        from odc.algo._masking import _or_fuser

        # Merge everything with OR first
        xx = xx.map(_or_fuser)

        # Ensure all 3 bits are exclusive
        #  bad=T, wet=?, dry=? => (wet'=F  , dry'=F)
        #  bad=F, wet=T, dry=T => (wet'=F  , dry'=F)
        #  else                => (wet'=wet, dry'=dry)
        wet = apply_numexpr("wet & (~dry) & (~bad)", xx, dtype="bool")
        dry = apply_numexpr("dry & (~wet) & (~bad)", xx, dtype="bool")

        return xr.Dataset(dict(wet=wet, dry=dry, bad=xx.bad, some=xx.some))

    def input_data(self, task: Task) -> xr.Dataset:
        chunks = {"y": -1, "x": -1}
        groupby = "solar_day"

        xx = load_with_native_transform(
            task.datasets,
            bands=["water"],
            geobox=task.geobox,
            native_transform=self._native_tr,
            fuser=self._fuser,
            groupby=groupby,
            resampling=self.resampling,
            chunks=chunks,
        )

        return xx

    def reduce(self, xx: xr.Dataset) -> xr.Dataset:
        nodata = -999
        count_some = xx.some.sum(axis=0, dtype="int16")
        count_wet = xx.wet.sum(axis=0, dtype="int16")
        count_dry = xx.dry.sum(axis=0, dtype="int16")
        count_clear = count_wet + count_dry
        frequency = safe_div(count_wet, count_clear, dtype="float32")

        count_wet.attrs["nodata"] = nodata
        count_clear.attrs["nodata"] = nodata

        is_ok = count_some > 0
        count_wet = keep_good_only(count_wet, is_ok)
        count_clear = keep_good_only(count_clear, is_ok)

        return xr.Dataset(
            dict(count_wet=count_wet, count_clear=count_clear, frequency=frequency,)
        )

    def rgba(self, xx: xr.Dataset) -> Optional[xr.DataArray]:
        return None


_plugins.register("wofs-summary", StatsWofs)
