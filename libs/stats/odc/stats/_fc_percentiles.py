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


NODATA = 255


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
        _measurments = [f"{b}_pc_{p}" for b, p in product(["pv", "bs", "npv"], ["10", "50", "90"])]
        _measurments.append("qa")
        return _measurments

    @staticmethod
    def _native_tr(xx):
        """
        Loads data in its native projection. It performs the following:

        1. Load all fc and WOfS bands
        2. Set the high terrain slope flag to 0
        3. Set all pixels that are not clear and dry to NODATA
        4. Calculate the clear wet pixels
        5. Drop the WOfS band
        """
        
        water = xx.water & 0b1110_1111
        dry = water == 0
        xx = xx.drop_vars(["water"])
        xx = keep_good_only(xx, dry, nodata=NODATA)
        xx["wet"] = water == 128
        return xx

    @staticmethod
    def _fuser(xx):

        wet = xx["wet"]
        xx = _xr_fuse(xx.drop_vars(["wet"]), partial(_fuse_mean_np, nodata=NODATA), '')

        band, *bands = xx.data_vars.keys()
        all_bands_invalid = xx[band] == NODATA
        for band in bands:
            all_bands_invalid &= xx[band] == NODATA

        xx["wet"] = _xr_fuse(wet, _fuse_or_np, wet.name) & all_bands_invalid
        return xx
    
    def input_data(self, task: Task) -> xr.Dataset:

        chunks = {"y": -1, "x": -1}

        xx = load_with_native_transform(
            task.datasets,
            bands=["water", "pv", "bs", "npv"],
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
        # (!all_bands_valid) & is_ever_wet => 0
        # (!all_bands_valid) & (!is_ever_wet) => 1
        # all_bands_valid => 2  

        wet = xx["wet"]
        xx = xx.drop_vars(["wet"])

        yy = xr_percentile(xx, [0.1, 0.5, 0.9], nodata=NODATA)
        is_ever_wet = _or_fuser(wet).squeeze(wet.dims[0], drop=True)

        band, *bands = yy.data_vars.keys()
        all_bands_valid = yy[band] != NODATA
        for band in bands:
            all_bands_valid &= yy[band] != NODATA

        all_bands_valid = all_bands_valid.astype(np.uint8)
        is_ever_wet = is_ever_wet.astype(np.uint8)
        yy["qa"] = 1 + all_bands_valid - is_ever_wet * (1 - all_bands_valid)
        return yy

    def rgba(self, xx: xr.Dataset) -> Optional[xr.DataArray]:
        return None


_plugins.register("fc-percentiles", StatsFCP)
