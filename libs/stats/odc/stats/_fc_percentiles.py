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
from odc.algo._masking import _xr_fuse, _or_fuser, _first_valid_np, _fuse_or_np, _fuse_and_np
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
        _measurments = [f"{b}_pc_{p}" for b, p in product(["pv", "bs", "npv", "ue"], ["10", "50", "90"])]
        _measurments.append("qa")
        return _measurments

    @staticmethod
    def _native_tr(xx):
        """
        Loads in the data in the native projection. It performs the following:

        1. Loads all the fc and WOfS bands
        2. Set high slope terrain flag to 0
        3. Extracts the clear dry and clear wet flags from WOfS
        4. Drops the WOfS band
        5. Masks out all pixels that are not clear and dry to a nodata value of 255
        6. Discards the clear dry flags
        """

        # set terrain flag to zero
        water = da.bitwise_and(xx["water"], 0b11101111)
        xx = xx.drop_vars(["water"])

        xx["dry"] = water == 0
        xx["wet"] = water == 128
        return xx

    @staticmethod
    def _fuser(xx):

        wet = xx.wet
        dry = xx.dry
        xx = _xr_fuse(xx.drop_vars(["wet", "dry"]), partial(_first_valid_np, nodata=NODATA), '')
        xx["wet"] = _xr_fuse(wet, _fuse_or_np, wet.name)
        xx["dry"] = _xr_fuse(dry, _fuse_and_np, wet.name)
        return xx

    def input_data(self, task: Task) -> xr.Dataset:

        chunks = {"y": -1, "x": -1}

        xx = load_with_native_transform(
            task.datasets,
            bands=["water", "pv", "bs", "npv", "ue"],
            geobox=task.geobox,
            native_transform=self._native_tr,
            fuser=self.fuser,
            groupby="solar_day",
            resampling=self.resampling,
            chunks=chunks,
        )
        
        return xx

    @staticmethod
    def reduce(xx: xr.Dataset) -> xr.Dataset: 
        # not 255 & True => 1
        # not 255 & False => 1
        # 255 & False => 2  
        # 255 + True => 3

        mask = xx["dry"]
        wet = xx["wet"]
        xx.drop_vars(["dry", "wet"])
        xx = keep_good_only(xx, mask, nodata=NODATA)

        yy = xr_percentile(xx, [0.1, 0.5, 0.9], nodata=NODATA)
        is_ever_wet = _or_fuser(wet).squeeze(wet.dims[0], drop=True)

        band, *bands = yy.data_vars.keys()
        all_bands_valid = yy[band] != NODATA
        for band in bands:
            all_bands_valid &= yy[band] != NODATA

        yy["qa"] = (1 - all_bands_valid) * (1 + is_ever_wet)
        
        return yy

    def rgba(self, xx: xr.Dataset) -> Optional[xr.DataArray]:
        return None


_plugins.register("fc-percentiles", StatsFCP)
