"""
Landsat pixel quality
"""
from functools import partial
from typing import List, Optional, Tuple, cast

import dask.array as da
import xarray as xr

from odc.algo import enum_to_bool, mask_cleanup
from odc.algo._masking import _or_fuser, keep_good_only, _xr_fuse, _first_valid_np, _fuse_or_np, _fuse_and_np
from odc.algo.io import load_with_native_transform
from odc.stats.model import Task

from .model import StatsPluginInterface
from . import _plugins


class StatsPQLSBitmask(StatsPluginInterface):
    NAME = "pc_ls_bitmask"
    SHORT_NAME = NAME
    VERSION = '0.0.1'
    PRODUCT_FAMILY = "pixel_quality_statistics"

    def __init__(
        self,
        mask_band: str = "QA_PIXEL",
        aerosol_band: str = None,
        filters: Optional[List[Tuple[int, int]]] = [(1, 2)],
        resampling: str = "nearest",
    ):
        self.filters = filters
        self.mask_band = mask_band
        self.aerosol_band = aerosol_band
        self.resampling = resampling

    @property
    def measurements(self) -> Tuple[str, ...]:
        band = [
            "clear",
            *[f"clear_{r1:d}_{r2:d}" for (r1, r2) in self.filters],
        ]
        if self.aerosol_band is not None:
            band.append("clear_aerosol")

        return tuple(band)

    def input_data(self, task: Task) -> xr.Dataset:
        chunks = {"y": -1, "x": -1}

        xx = load_with_native_transform(
            task.datasets,
            band = self.mask_band,
            geobox=task.geobox,
            native_transform=self._native_tr,
            fuser=self._fuser,
            groupby="solar_day",
            resampling=self.resampling,
            chunks=chunks,
        )

        if self.aerosol_band is None:
            return xx

        aerosol_data = load_with_native_transform(
            task.datasets,
            band = self.aerosol_band,
            geobox=task.geobox,
            native_transform=self._aerosol_native_tr,
            fuser=self._aerosol_fuser,
            groupby="solar_day",
            resampling=self.resampling,
            chunks=chunks,
        )
        return xr.concat([xx, aerosol_data])

    def reduce(self, xx: xr.Dataset) -> xr.Dataset:
        clear_bands = [str(n) for n in xx.data_vars if str(n).startswith("clear")]

        for band in clear_bands:


        return xx

    def _native_tr(self, xx: xr.Dataset) -> xr.Dataset:
        """
        Loads in the data in the native projection and perform transform
        """
        mask_band = xx["mask_band"]
        xx = xx.drop_vars(mask_band)

        clear_mask = da.bitwise_and(mask_band, 0b0000_0000_0001_1010) == 0 # True=clear
        keeps = da.bitwise_and(mask_band, 0b0000_0000_0000_0001) == 0 # True=data

        # drops nodata pixels
        xx = keep_good_only(xx, keeps)

        xx["clear"] = clear_mask

        return xx

    def _fuser(self, xx: xr.Dataset) -> xr.Dataset:
        """
        Fuser clear with OR, and apply mask_cleanup
        """
        clear_mask = xx["clear"]
        xx = _xr_fuse(xx.drop_vars(["clear"]), partial(_first_valid_np, nodata=0), '')
        xx["clear"] = _xr_fuse(clear_mask, _fuse_or_np, clear_mask.name)

        # apply filters - [r1, r2]
        # r1 = shrinks away small areas of the mask
        # r2 = adds padding to the mask
        for r1, r2 in self.filters:
            xx[f"clear_{r1:d}_{r2:d}"] = mask_cleanup(xx["clear"], (r1,r2))

        return xx

    def _aerosol_native_tr(self, xx: xr.Dataset) -> xr.Dataset:
        """
        Loads in the data in the native projection and perform transform
        """
        mask_band = xx["mask_band"]
        xx = xx.drop_vars(mask_band)

        aerosol_level = da.bitwise_and(mask_band, 0b1100_0000)/64
        clear_aerosol_mask = aerosol_level != 3
        keeps = da.bitwise_and(mask_band, 0b0000_0001) == 0 # True=data

        # drops nodata pixels
        xx = keep_good_only(xx, keeps)

        xx["clear_aerosol"] = clear_aerosol_mask

        return xx

    def _fuser(self, xx: xr.Dataset) -> xr.Dataset:
        """
        Fuser clear with OR, and apply mask_cleanup
        """
        clear_mask = xx["clear"]
        xx = _xr_fuse(xx.drop_vars(["clear"]), partial(_first_valid_np, nodata=0), '')
        xx["clear"] = _xr_fuse(clear_mask, _fuse_or_np, clear_mask.name)

        # apply filters - [r1, r2]
        # r1 = shrinks away small areas of the mask
        # r2 = adds padding to the mask
        for r1, r2 in self.filters:
            xx[f"clear_{r1:d}_{r2:d}"] = mask_cleanup(xx["clear"], (r1,r2))


_plugins.register("pq", StatsPQLSBitmask)
