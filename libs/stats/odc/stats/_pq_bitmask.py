"""
USGS Landsat pixel quality
"""
from functools import partial
from typing import List, Optional, Tuple

import dask.array as da
import xarray as xr

from odc.algo import mask_cleanup, keep_good_only
from odc.algo._masking import _xr_fuse, _first_valid_np, _fuse_or_np
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
        """
        Output product measurements
        """
        _measurements = [
            "total",
            "clear",
            *[f"clear_{r1:d}_{r2:d}" for (r1, r2) in self.filters],
        ]
        if self.aerosol_band is not None:
            _measurements.append("clear_aerosol")

        return tuple(_measurements)

    def input_data(self, task: Task) -> xr.Dataset:
        bands = [self.pq_band]
        if self.aerosol_band is not None:
            bands.append(self.aerosol_band)

        return load_with_native_transform(
            task.datasets,
            bands=bands,  # measurements to load
            geobox=task.geobox,
            native_transform=self._native_tr,
            fuser=self._fuser,
            groupby="solar_day",
            resampling=self.resampling,
            chunks={"x": -1, "y": -1},
        )

    def reduce(self, xx: xr.Dataset) -> xr.Dataset:
        pq = xr.Dataset()

        # apply filters to clear mask - [(r1, r2)]
        #  r1 = shrinks away small areas of the mask
        #  r2 = adds padding to the mask
        for r1, r2 in self.filters:
            xx[f"clear_{r1:d}_{r2:d}"] = mask_cleanup(xx["clear"], (r1, r2))

        # calculate pq - total, clear, clear_<filter>, clear_aerosol(if applicable) pixel counts
        clear_bands = [str(n) for n in xx.data_vars if str(n).startswith("clear")]
        keeps_band = xx["keeps"]  # band with nodata mask
        pq["total"] = keeps_band.sum(axis=0, dtype="uint16")
        for band in clear_bands:
            pq[band] = (xx[band] & keeps_band).sum(axis=0, dtype="uint16")

        return pq

    def _native_tr(self, xx: xr.Dataset) -> xr.Dataset:
        """
        Loads the data in the native projection and perform transform
        """
        pq_band = xx[self.pq_band]
        xx = xx.drop_vars(pq_band)

        clear_mask = da.bitwise_and(pq_band, 0b0000_0000_0001_1010) == 0  # True=clear
        keeps = da.bitwise_and(pq_band, 0b0000_0000_0000_0001) == 0  # True=data

        if self.aerosol_band is not None:
            aerosol_band = xx[self.aerosol_band]
            xx = xx.drop_vars(aerosol_band)

            aerosol_level = da.bitwise_and(aerosol_band, 0b1100_0000) / 64
            clear_aerosol_mask = aerosol_level != 3

        # drops nodata pixels
        xx = keep_good_only(xx, keeps)

        xx["keeps"] = keeps
        xx["clear"] = clear_mask
        if self.aerosol_band is not None:
            xx["clear_aerosol"] = clear_aerosol_mask

        return xx

    def _fuser(self, xx: xr.Dataset) -> xr.Dataset:
        """
        Fuser masking bands with OR
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

        return fuser_result


_plugins.register("pq", StatsPQLSBitmask)
