"""
Fractional Cover Percentiles
"""
from typing import Optional, Tuple
from itertools import product
import xarray as xr
from odc.stats.model import Task
from odc.algo.io import load_with_native_transform
from odc.algo import keep_good_only
from odc.algo._percentile import xr_percentile
from odc.algo._masking import _or_fuser
from .model import StatsPluginInterface
from . import _plugins


class StatsFCP(StatsPluginInterface):
    
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
        _measurments = [f"{b}_pc_{p}" for b, p in product(["pv", "bs", "npv", "ue"], ["10", "50", "90"])]
        _measurments.append("is_ever_wet")
        return _measurments

    @staticmethod
    def _native_tr(xx):
        """
        Loads in the data in the native projection. It performs the following:

        1. Loads all the fc and WOfS bands
        2. Extracts the clear dry and clear wet flags from WOfS
        3. Drops the WOfS band
        4. Stores the clear wet flags for QA later on 
        5. Masks out all pixels that are not clear and dry to a nodata value of 255
        6. Discards the clear dry flags
        """
        # use the dry flag to indicate good measurementd
        dry = xx.water == 0

        # keep the clear wet measurements to calculate the QA band in reduce
        xx["wet"] = xx.water == 128
        xx = xx.drop_vars(["water"])
        return keep_good_only(xx, dry, nodata=255)

    def input_data(self, task: Task) -> xr.Dataset:

        chunks = {"y": -1, "x": -1}

        xx = load_with_native_transform(
            task.datasets,
            bands=["water", "pv", "bs", "npv", "ue"],
            geobox=task.geobox,
            native_transform=self._native_tr,
            fuser=None,
            groupby=None,
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
