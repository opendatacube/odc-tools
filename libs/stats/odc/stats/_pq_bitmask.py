"""
USGS Landsat pixel quality

pq_band = input band for cloud masking
aerosol_band = input band for aerosol masking
filters = filters to apply on cloud mask - [[r1, r2, r3], ...]
    r1 = shrinks away small areas of the mask
    r2 = adds padding to the mask
    r3 = remove small holes in cloud - morphological closing
aerosol_filters = filters to apply on cloud mask - [[r1, r2, r3], ...] and then calculate clear_aerosol
resampling = "nearest"
"""

from functools import partial
from typing import List, Optional, Tuple

import dask.array as da
import xarray as xr

from odc.algo import mask_cleanup, keep_good_only
from odc.algo._masking import _xr_fuse, _first_valid_np, _fuse_or_np, _fuse_and_np, binary_closing
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
            aerosol_band: Optional[str] = None,
            filters: Optional[List[Tuple[int, int, int]]] = [],
            aerosol_filters: Optional[List[Tuple[int, int, int]]] = [],
            resampling: str = "nearest",
    ):
        self.pq_band = pq_band
        self.aerosol_band = aerosol_band
        self.filters = filters
        self.aerosol_filters = aerosol_filters
        self.resampling = resampling

    @property
    def measurements(self) -> Tuple[str, ...]:
        """
        Output product measurements
        """
        _measurements = [
            "total",
            "clear",
            *[f"clear_{r1:d}_{r2:d}_{r3:d}" for (r1, r2, r3) in self.filters],
        ]
        if self.aerosol_band and self.aerosol_band=="SR_QA_AEROSOL":
            aerosol_measurements = [
                "clear_aerosol",
                *[f"clear_{r1:d}_{r2:d}_{r3:d}_aerosol" for (r1, r2, r3) in self.aerosol_filters if self.aerosol_filters],
            ]
            _measurements.extend(aerosol_measurements)

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
        """
        calculate pixel count:
        pq bands:
            total                  -> total pixel count (valid data)
            clear                  -> count clear_cloud (pixels without cloud)
            clear_<filter>         -> apply filter on erased_mask (cloud mask) and then count clear_cloud
            clear_aerosol          -> count clear_cloud + clear_aerosol
            clear_<filter>_aerosol -> count clear_cloud_filter + clear_aerosol
        """
        pq = xr.Dataset()

        for r1, r2, r3 in self.filters or []:
            cloud_mask = binary_closing(xx["erased"], r3)
            xx[f"erased_{r1:d}_{r2:d}_{r3:d}"] = mask_cleanup(cloud_mask, (r1, r2))

        erased_bands = [str(n) for n in xx.data_vars if str(n).startswith("erased")]
        valid = xx["keeps"]
        pq["total"] = valid.sum(axis=0, dtype="uint16")
        for band in erased_bands:
            clear_name = band.replace("erased", "clear")
            if "aerosol" in band:
                pq[clear_name] = (valid & (~xx[band] * ~xx["erased"])).sum(axis=0, dtype="uint16")
            else:
                pq[clear_name] = (valid & (~xx[band])).sum(axis=0, dtype="uint16")

        if self.aerosol_band and self.aerosol_band=="SR_QA_AEROSOL":
            for r1, r2, r3 in self.aerosol_filters or []:
                # apply filter on cloud_mask if not exists
                if f"erased_{r1:d}_{r2:d}_{r3:d}" not in xx:
                    cloud_mask = binary_closing(xx["erased"], r3)
                    xx[f"erased_{r1:d}_{r2:d}_{r3:d}"] = mask_cleanup(cloud_mask, (r1, r2))

                pq[f"clear_{r1:d}_{r2:d}_{r3:d}_aerosol"] = (valid & (~xx[f"erased_{r1:d}_{r2:d}_{r3:d}"] & ~xx["erased_aerosol"])).sum(axis=0, dtype="uint16")

        return pq

    def _native_tr(self, xx: xr.Dataset) -> xr.Dataset:
        """
        Loads the data in the native projection and perform transform
        bands:
            keeps          -> anything but nodata (valid pixels)
            erased         -> cloudy pixels
            erased_aerosol -> high aerosol pixels
        """
        pq_band = xx[self.pq_band]
        xx = xx.drop_vars([self.pq_band])

        # set bitmask
        cloud_mask = da.bitwise_and(pq_band, 0b0000_0000_0001_1010) != 0   # True=cloud
        keeps = da.bitwise_and(pq_band, 0b0000_0000_0000_0001) == 0  # True=data

        if self.aerosol_band and self.aerosol_band=="SR_QA_AEROSOL":
            aerosol_band = xx[self.aerosol_band]
            xx = xx.drop_vars([self.aerosol_band])

            # set aerosol_level
            aerosol_level = da.bitwise_and(aerosol_band, 0b1100_0000) / 64

        # drops nodata pixels
        xx = keep_good_only(xx, keeps)

        xx["keeps"] = keeps
        xx["erased"] = cloud_mask
        if self.aerosol_band:
            xx["erased_aerosol"] = aerosol_level == 3

        return xx

    def _fuser(self, xx: xr.Dataset) -> xr.Dataset:
        """
        Fuser cloud and aerosol masking bands with OR
        """
        cloud_mask = xx["erased"]
        xx = xx.drop_vars(["erased"])
        if self.aerosol_band and self.aerosol_band=="SR_QA_AEROSOL":
            high_aerosol_mask = xx["erased_aerosol"]
            xx = xx.drop_vars(["erased_aerosol"])

        fuser_result = _xr_fuse(xx, partial(_first_valid_np, nodata=0), '')
        fuser_result["erased"] = _xr_fuse(cloud_mask, _fuse_or_np, cloud_mask.name)
        if self.aerosol_band and self.aerosol_band=="SR_QA_AEROSOL":
            fuser_result["erased_aerosol"] = _xr_fuse(high_aerosol_mask, _fuse_or_np, high_aerosol_mask.name)

        return fuser_result


_plugins.register("pq-ls-bitmask", StatsPQLSBitmask)
