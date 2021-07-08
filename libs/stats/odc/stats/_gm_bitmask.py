"""
Fractional Cover Percentiles
"""
from functools import partial
from typing import Optional, Tuple

import dask.array as da
import xarray as xr
from odc.algo import geomedian_with_mads, keep_good_only
from odc.algo._masking import _xr_fuse, _first_valid_np, _fuse_and_np
from odc.algo.io import load_with_native_transform
from odc.stats.model import Task

from . import _plugins
from .model import StatsPluginInterface


class StatsGMLSBitMask(StatsPluginInterface):
    NAME = "gm_ls_annual"
    SHORT_NAME = NAME
    VERSION = "3.0.0"
    PRODUCT_FAMILY = "geomedian"

    def __init__(
            self,
            bands: Optional[Tuple[str, ...]] = None,
            mask_band: str = "QA_PIXEL",
            aux_names=dict(smad="sdev", emad="edev", bcmad="bcdev", count="count"),
            rgb_bands=None,
            resampling: str = "bilinear",
            work_chunks: Tuple[int, int] = (400, 400),
            **other,
    ):
        self.mask_band = mask_band
        self.resampling = resampling
        self.bands = bands
        self.work_chunks = work_chunks
        self.renames = aux_names
        self.aux_bands = list(aux_names.values())

        if bands is None:
            bands = (
                "red",
                "green",
                "blue",
                "nir",
                "swir1",
                "swir2",
            )
            if rgb_bands is None:
                rgb_bands = ("red", "green", "blue")

    @property
    def measurements(self) -> Tuple[str, ...]:
        return self.bands + self.aux_bands

    def _native_tr(self, xx):
        """
        Loads in the data in the native projection. It performs the following:

        1. Loads pq bands
        2. Extract cloud_mask flags from bands
        3. Add cloud_mask
        4. Drops nodata pixels

        .. bitmask::
            7   6   5   4   3   2   1   0
            |   |   |   |   |   |   |   |
            |   |   |   |   |   |   |   x-----> nodata
            |   |   |   |   |   |   o---------> dilated_cloud
            |   |   |   |   |   x-------------> cirrus
            |   |   |   |   o-----------------> cloud
            |   |   |   x---------------------> cloud_shadow
            |   |   o-------------------------> snow
            |   x-----------------------------> clear
            o---------------------------------> water
        """
        mask_band = xx[self.mask_band]
        xx = xx.drop_vars([self.mask_band])

        # set cloud_mask (cloud + cloud_shadow) bitmask
        cloud_mask = da.bitwise_and(mask_band, 0b00011000) == 0
        xx["cloud_mask"] = cloud_mask

        # set no_data bitmask
        keeps = da.bitwise_and(mask_band, 0b00000001) == 0

        # drops nodata pixels
        xx = keep_good_only(xx, keeps)

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
        scale = 1 / 10_000
        cfg = dict(
            maxiters=1000,
            num_threads=1,
            scale=scale,
            offset=-1 * scale,
            reshape_strategy="mem",
            out_chunks=(-1, -1, -1),
            work_chunks=self.work_chunks,
            compute_count=True,
            compute_mads=True,
        )

        cloud_mask = xx["cloud_mask"]
        xx = xx.drop_vars(["cloud_mask"])

        # keeping only non cloud pixels
        xx = keep_good_only(xx, cloud_mask)

        gm = geomedian_with_mads(xx, **cfg)
        gm = gm.rename(self.renames)

        return gm

    def _fuser(self, xx):
        cloud_mask = xx["cloud_mask"]
        xx = _xr_fuse(xx.drop_vars(["cloud_mask"]), partial(_first_valid_np, nodata=0), '')
        xx["cloud_mask"] = _xr_fuse(cloud_mask, _fuse_and_np, cloud_mask.name)

        return xx

    def rgba(self, xx: xr.Dataset) -> Optional[xr.DataArray]:
        return None


_plugins.register("gm-ls-bit-mask", StatsGMLSBitMask)
