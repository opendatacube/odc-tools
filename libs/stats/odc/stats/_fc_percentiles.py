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
from odc.algo._masking import _fuse_with_custom_op, _or_fuser, _first_valid_np, _fuse_or_np
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
        2. Set high slope terrain flag to 0
        3. Extracts the clear dry and clear wet flags from WOfS
        4. Drops the WOfS band
        5. Masks out all pixels that are not clear and dry to a nodata value of 255
        6. Discards the clear dry flags
        """

        # set terrain flag to zero
        water = da.bitwise_and(xx["water"], 0b11101111)
        xx = xx.drop_vars(["water"])

        # use the dry flag to indicate good measurements
        dry = water == 0
        xx = keep_good_only(xx, dry, nodata=255)
        xx["wet"] = water == 128
        return xx

    @staticmethod
    def _fuser(xx):
        variables = {}
        data_bands = [band for band in xx.data_vars.keys() if band != "wet"]
        for band in data_bands:
            variables[band] = _fuse_with_custom_op(xx[band], partial(_first_valid_np, nodata=255))
        variables["wet"] = _fuse_with_custom_op(xx["wet"], _fuse_or_np)
        
        for k, v in variables.items():
            v._copy_attrs_from(xx.data_vars[k])

        yy = type(xx)(variables, attrs=xx.attrs)
        return yy

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

        yy = xr_percentile(xx.drop_vars(["wet"]), [0.1, 0.5, 0.9], nodata=255)
        is_ever_wet = _or_fuser(xx["wet"]).squeeze(xx["wet"].dims[0], drop=True)

        band, *bands = [band for band in yy.data_vars.keys() if band != "wet"]
        all_bands_valid = yy[band] != 255
        for band in bands:
            all_bands_valid &= yy[band] != 255

        yy["qa"] = (1 - all_bands_valid) * (1 + is_ever_wet)
        
        return yy

    def rgba(self, xx: xr.Dataset) -> Optional[xr.DataArray]:
        return None


_plugins.register("fc-percentiles", StatsFCP)
