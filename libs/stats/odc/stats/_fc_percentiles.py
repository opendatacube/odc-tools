"""
Fractional Cover Percentiles
"""
from typing import Optional, Tuple
import xarray as xr
from odc.stats.model import Task
from odc.algo.io import load_with_native_transform
from odc.algo import keep_good_only
from odc.algo._percentile import xr_percentile
from odc.algo._masking import _or_fuser
from .model import StatsPluginInterface
from . import _plugins


class StatsFCP(StatsPluginInterface):
    """
    """
    NAME = "ga_fc_percentiles"
    SHORT_NAME = NAME
    VERSION = "0.0.1"
    PRODUCT_FAMILY = "fc_percentiles"

    def __init__(
        self,
        resampling: str = "bilinear",
    ):
        self.resampling = resampling

    @property
    def measurements(self) -> Tuple[str, ...]:
        return "count_wet", "count_clear", "frequency"

    @staticmethod
    def _native_tr(xx):
        """
        
        """
        
        # use the dry flag to indicate good measurementd
        dry = xx.water == 0

        # keep the clear wet measurements to calculate the QA band in reduce
        xx["wet"] = xx.water == 128
        xx = xx.drop_vars(["water"])
        return keep_good_only(xx, dry, nodata=255)

    def input_data(self, task: Task) -> xr.Dataset:
        chunks = {"y": -1, "x": -1}
        groupby = "solar_day"

        xx = load_with_native_transform(
            task.datasets,
            bands=["water", "pv", "bs", "npv", "ue"],
            geobox=task.geobox,
            native_transform=self._native_tr,
            fuser=None,
            groupby=groupby,
            resampling=self.resampling,
            chunks=chunks,
        )

        return xx

    def reduce(self, xx: xr.Dataset) -> xr.Dataset:        
        yy = xr_percentile(xx.drop_vars(["wet"]), [0.1, 0.5, 0.9], nodata=255)
        yy["is_ever_wet"] = _or_fuser(xx["wet"]).squeeze(xx["wet"].dims[0], drop=True)
        return yy

    def rgba(self, xx: xr.Dataset) -> Optional[xr.DataArray]:
        return None


_plugins.register("fc-percentiles", StatsFCP)
