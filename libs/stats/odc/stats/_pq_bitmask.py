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
        pq_band: str = "QA_PIXEL",
        aerosol_band: str = None,
        filters: Optional[List[Tuple[int, int]]] = [(1, 2)],
        resampling: str = "nearest",
    ):
        self.filters = filters
        self.pq_band = pq_band
        self.aerosol_band = aerosol_band
        self.resampling = resampling

    @property
    def measurements(self) -> Tuple[str, ...]:
        band = [
            "total",
            "clear",
            *[f"clear_{r1:d}_{r2:d}" for (r1, r2) in self.filters],
        ]
        if self.aerosol_band is not None:
            band.append("clear_aerosol")

        return tuple(band)

    def input_data(self, task: Task) -> xr.Dataset:
        chunks = {"y": -1, "x": -1}
        bands = [self.pq_band]
        if self.aerosol_band is not None:
            bands.append(self.aerosol_band)

        xx = load_with_native_transform(
            task.datasets,
            bands = bands,
            geobox=task.geobox,
            native_transform=self._native_tr,
            fuser=self._fuser,
            groupby="solar_day",
            resampling=self.resampling,
            chunks=chunks,
        )

        return xx

    def reduce(self, xx: xr.Dataset) -> xr.Dataset:
        total_pixels = xx.keeps
        total = total_pixels.sum(axis=0, dtype="uint16")
        pq = xr.Dataset(dict(total=total))

        # collect all bands starts with clear and calculate its count
        clear_bands = [str(n) for n in xx.data_vars if str(n).startswith("clear")]
        for band in clear_bands:
            pixel_count = xx[band].sum(axis=0, dtype="uint16")
            pq[band] = pixel_count

        return pq

    def _native_tr(self, xx: xr.Dataset) -> xr.Dataset:
        """
        Loads the data in the native projection and perform transform
        """
        pq_band = xx[self.pq_band]
        xx = xx.drop_vars(pq_band)

        clear_mask = da.bitwise_and(pq_band, 0b0000_0000_0001_1010) == 0 # True=clear
        keeps = da.bitwise_and(pq_band, 0b0000_0000_0000_0001) == 0 # True=data

        if self.aerosol_band is not None:
            aerosol_band = xx[self.aerosol_band]
            xx = xx.drop_vars(aerosol_band)

            aerosol_level = da.bitwise_and(aerosol_band, 0b1100_0000)/64
            clear_aerosol_mask = aerosol_level != 3

        xx["keeps"] = keeps
        xx["clear"] = clear_mask
        if self.aerosol_band is not None:
            xx["clear_aerosol"] = clear_aerosol_mask

        return xx

    def _fuser(self, xx: xr.Dataset) -> xr.Dataset:
        """
        Fuser masking bands with OR, and apply mask_cleanup if requested
        """
        clear_mask = xx["clear"]
        xx = xx.drop_vars(["clear"])
        if self.aerosol_band is not None:
            clear_aerosol_mask = xx["clear_aerosol"]
            xx = xx.drop_vars(["clear_aerosol"])

        fuser_result = _xr_fuse(xx, partial(_first_valid_np, nodata=0), '')
        fuser_result["clear"] = _xr_fuse(clear_mask, _fuse_or_np, clear_mask.name)
        if self.aerosol_band is not None:
            fuser_result["clear_aerosol"] = _xr_fuse(clear_aerosol_mask, _fuse_or_np, clear_aerosol_mask.name)

        # apply filters - [r1, r2]
        # r1 = shrinks away small areas of the mask
        # r2 = adds padding to the mask
        for r1, r2 in self.filters:
            fuser_result[f"clear_{r1:d}_{r2:d}"] = mask_cleanup(clear_mask, (r1,r2))

        return fuser_result


_plugins.register("pq", StatsPQLSBitmask)
