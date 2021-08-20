"""
Fractional Cover Percentiles
"""
from functools import partial
from typing import Optional, Tuple
from itertools import product
import dask.array as da
import xarray as xr
import numpy as np
from odc.stats.model import Task
from odc.algo.io import load_with_native_transform
from odc.algo import keep_good_only
from odc.algo._percentile import xr_percentile
from odc.algo._masking import _xr_fuse, _or_fuser, _fuse_mean_np, _fuse_or_np, _fuse_and_np
from .model import StatsPluginInterface
from . import _plugins


NODATA = -9999 # output NODATA


class StatsTCWPC(StatsPluginInterface):
    
    NAME = "ga_tcw_percentiles"
    SHORT_NAME = NAME
    VERSION = "0.0.1"
    PRODUCT_FAMILY = "twc_percentiles"

    def __init__(
        self,
        resampling: str = "bilinear",
    ):
        self.resampling = resampling

    @property
    def measurements(self) -> Tuple[str, ...]:
        _measurments = ["tcw_pc_10", "tcw_pc_50", "tcw_pc_90"]
        _measurments
        return _measurments

    @staticmethod
    def _native_tr(xx):
        """
        Loads data in its native projection.
        """

        bad = ((xx["fmask"] & 0b0000_1101) == 0)
        xx = xx.drop_vars(["fmask"])
        
        for band in xx.data_vars.keys():
            bad = bad | (xx[band] == -999)

        tcw = 0.0135 * xx['blue'] + 0.2021 * xx['green'] + 0.3102 * xx['red'] + 0.1584 * xx['nir'] - 0.6806 * xx['swir1'] - 0.6109 * xx['swir2'] 
        
        xx = xx.drop_vars(xx.data_vars.keys())
        xx['tcw'] = tcw.astype(np.int16)
        xx = keep_good_only(xx, ~bad, nodata=NODATA)
        return xx

    @staticmethod
    def _fuser(xx):

        xx = _xr_fuse(xx, partial(_fuse_mean_np, nodata=NODATA), '')

        return xx
    
    def input_data(self, task: Task) -> xr.Dataset:

        chunks = {"y": -1, "x": -1}

        xx = load_with_native_transform(
            task.datasets,
            bands=["blue", "green", "red", "nir", "swir1", "swir2", "fmask"],
            geobox=task.geobox,
            native_transform=self._native_tr,
            fuser=self._fuser,
            groupby="solar_day",
            resampling=self.resampling,
            chunks=chunks,
        )
        
        return xx

    @staticmethod
    def reduce(xx: xr.Dataset) -> xr.Dataset:

        yy = xr_percentile(xx, [0.1, 0.5, 0.9], nodata=NODATA)
        return yy

    def rgba(self, xx: xr.Dataset) -> Optional[xr.DataArray]:
        return None


_plugins.register("tcw-percentiles", StatsTCWPC)
