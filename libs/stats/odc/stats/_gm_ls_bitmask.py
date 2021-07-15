"""
Landsat QA Pixel Geomedian
"""
from functools import partial
from typing import Optional, Tuple

import dask.array as da
import xarray as xr
import numpy as np
from odc.algo import geomedian_with_mads, keep_good_only, erase_bad, to_rgba
from odc.algo._masking import _xr_fuse, _first_valid_np, mask_cleanup, _fuse_or_np
from odc.algo.io import load_with_native_transform
from odc.stats.model import Task

from . import _plugins
from .model import StatsPluginInterface


class StatsGMLSBitmask(StatsPluginInterface):
    NAME = "gm_ls_bitmask"
    SHORT_NAME = NAME
    VERSION = "0.0.1"
    def __init__(
            self,
            bands: Optional[Tuple[str, ...]] = None,
            mask_band: str = "QA_PIXEL",
            filters: Optional[Tuple[int, int]] = None,
            aux_names=dict(smad="sdev", emad="edev", bcmad="bcdev", count="count"),
            resampling: str = "bilinear",
            work_chunks: Tuple[int, int] = (400, 400),
            **other,
    ):
        self.mask_band = mask_band
        self.resampling = resampling
        self.bands = bands
        self.filters = filters
        self.work_chunks = work_chunks
        self.renames = aux_names
        self.aux_bands = list(aux_names.values())

        if self.bands is None:
            self.bands = (
                "red",
                "green",
                "blue",
                "nir",
                "swir1",
                "swir2",
            )

    @property
    def measurements(self) -> Tuple[str, ...]:
        return self.bands + self.aux_bands

    def _native_tr(self, xx):
        """
        Loads in the data in the native projection. It performs the following:

        1. Loads pq bands
        2. Extract cloud_mask flags from bands
        3. Drops nodata pixels
        4. Add cloud_mask

        .. bitmask::
            15  14  13  12  11  10  9   8   7   6   5   4   3   2   1   0
            |   |   |   |   |   |   |   |   |   |   |   |   |   |   |   |
            |   |   |   |   |   |   |   |   |   |   |   |   |   |   |   x-----> nodata
            |   |   |   |   |   |   |   |   |   |   |   |   |   |   o---------> dilated_cloud
            |   |   |   |   |   |   |   |   |   |   |   |   |   x-------------> cirrus
            |   |   |   |   |   |   |   |   |   |   |   |   o-----------------> cloud
            |   |   |   |   |   |   |   |   |   |   |   x---------------------> cloud_shadow
            |   |   |   |   |   |   |   |   |   |   o-------------------------> snow
            |   |   |   |   |   |   |   |   |   x-----------------------------> clear
            |   |   |   |   |   |   |   |   o---------------------------------> water
            |   |   |   |   |   |   |   x-------------------------------------> cloud_confidence
            |   |   |   |   |   |   o-----------------------------------------> cloud_confidence
            |   |   |   |   |   x---------------------------------------------> cloud_shadow_confidence
            |   |   |   |   o-------------------------------------------------> cloud_shadow_confidence
            |   |   |   x-----------------------------------------------------> snow_ice_confidence
            |   |   o---------------------------------------------------------> snow_ice_confidence
            |   x-------------------------------------------------------------> cirrus_confidence
            0-----------------------------------------------------------------> cirrus_confidence
        """
        mask_band = xx[self.mask_band]
        xx = xx.drop_vars([self.mask_band])

        # set cloud_mask (dilated_cloud + cloud + cloud_shadow) bitmask - True=cloud, False=non-cloud
        cloud_mask = da.bitwise_and(mask_band, 0b0000_0000_0001_1010) != 0

        # set no_data bitmask - True=data, False=no-data
        keeps = da.bitwise_and(mask_band, 0b0000_0000_0000_0001) == 0

        # drops nodata pixels and add cloud_mask from xx
        xx = keep_good_only(xx, keeps)
        xx["cloud_mask"] = cloud_mask

        return xx

    def input_data(self, task: Task) -> xr.Dataset:

        chunks = {"y": -1, "x": -1}

        xx = load_with_native_transform(
            task.datasets,
            bands=self.bands + [self.mask_band],
            geobox=task.geobox,
            native_transform=self._native_tr,
            fuser=self._fuser,
            groupby="solar_day",
            resampling=self.resampling,
            chunks=chunks,
        )

        return xx

    def reduce(self, xx: xr.Dataset) -> xr.Dataset:
        scale = 0.0000275
        offset = -0.2
        return_SR = False
        cfg = dict(
            maxiters=1000,
            num_threads=1,
            scale=scale,
            offset=offset,
            return_SR=return_SR,
            reshape_strategy="mem",
            out_chunks=(-1, -1, -1),
            work_chunks=self.work_chunks,
            compute_count=True,
            compute_mads=True,
        )

        cloud_mask = xx["cloud_mask"]
        xx = xx.drop_vars(["cloud_mask"])

        # erase pixels with cloud
        xx = erase_bad(xx, cloud_mask)

        gm = geomedian_with_mads(xx, **cfg)
        gm = gm.rename(self.renames)

        # Rescale USGS Landsat bands into surface reflectance
        if return_SR:
            sr_bands = ['red', 'green', 'blue', 'nir', 'swir_1', 'swir_2']
            for band in gm.data_vars:
                if band in sr_bands:
                    #convert to surface reflectance (0-1)
                    ds[band] = 2.75e-5 * ds[band] - 0.2
                    # match Sentinel-2 scaling for consistency
                    ds[band] = ds[band] * 10000
                    #force dtype back to int
                    ds[band] = ds[band].astype(np.uint16)
        
        # TODO: handle edev scaling correctly
        # need to investigate what stats is doing 
        
        return gm

    def _fuser(self, xx):
        """
        Fuse cloud_mask with OR, and apply mask_cleanup if requested
        """
        cloud_mask = xx["cloud_mask"]
        xx = _xr_fuse(xx.drop_vars(["cloud_mask"]), partial(_first_valid_np, nodata=0), '')
        xx["cloud_mask"] = _xr_fuse(cloud_mask, _fuse_or_np, cloud_mask.name)

        # apply filters - [r1, r2]
        # r1 = shrinks away small areas of the mask
        # r2 = adds padding to the mask
        if self.filters is not None:
            xx["cloud_mask"] = mask_cleanup(xx["cloud_mask"], self.filters)

        return xx

    def rgba(self, xx: xr.Dataset) -> Optional[xr.DataArray]:
        return None


_plugins.register("gm-ls-bitmask", StatsGMLSBitmask)
