"""
USGS Landsat pixel quality

pq_band = input band for cloud masking
| Name | Units | Conversion | Description |
|------|-------|------------|-------------|
| QA_PIXEL | Bit Index | NA | Pixel quality; Bit: 0 = nodata; 1 = Dilated Cloud; 3 = cloud; 4 = cloud-shadow |

aerosol_band = input band for aerosol masking; provide one of the band as an input measurements
| Name | Units | Conversion | Description |
|------|-------|------------|-------------|
| SR_ATMOS_OPACITY | Unitless | 0.001 * DN | Atmospheric opacity; < 0.1 = clear; 0.1 - 0.3 = average; > 0.3 = hazy |
| SR_QA_AEROSOL    | Bit Index | NA | Aerosol level; Bit(6-7): 00 = climatology; 01 = low; 10 = medium; 11 = high |

filters = filters to apply on cloud mask - dict(closing=int, opening=int, dilation=int), where
    closing(optional = remove small holes in cloud - morphological closing
    opening = shrinks away small areas of the mask
    dilation = adds padding to the mask
aerosol_filters = filters to apply on cloud mask - dict(closing=int, opening=int, dilation=int) and then calculate clear_aerosol
resampling = "nearest"
"""

from functools import partial
from typing import Dict, List, Optional, Tuple

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
            aerosol_band: Optional[str] = None,
            filters: Optional[List[Dict[str, int]]] = None,
            aerosol_filters: Optional[List[Dict[str, int]]] = None,
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
        measurements = ["total", "clear"]
        for filter in self.filters or []:
            if "closing" in filter:
                measurements.append(f"clear_{filter['closing']:d}_{filter['opening']:d}_{filter['dilation']:d}")
            else:
                measurements.append(f"clear_{filter['opening']:d}_{filter['dilation']:d}")

        if self.aerosol_band:
            measurements.append("clear_aerosol")
            if self.aerosol_band == "SR_QA_AEROSOL":
                for aerosol_filter in self.aerosol_filters or []:
                    if "closing" in aerosol_filter:
                        measurements.append(f"clear_{aerosol_filter['closing']:d}_{aerosol_filter['opening']:d}_{aerosol_filter['dilation']:d}_aerosol")
                    else:
                        measurements.append(f"clear_{aerosol_filter['opening']:d}_{aerosol_filter['dilation']:d}_aerosol")

        return tuple(measurements)

    def input_data(self, task: Task) -> xr.Dataset:
        bands = [self.pq_band]
        if self.aerosol_band:
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

        for filter in self.filters or []:
            if "closing" in filter:
                erased_band_name = f"erased_{filter['closing']:d}_{filter['opening']:d}_{filter['dilation']:d}"
            else:
                erased_band_name = f"erased_{filter['opening']:d}_{filter['dilation']:d}"
            xx[erased_band_name] = mask_cleanup(xx["erased"], filter)

        erased_bands = [str(n) for n in xx.data_vars if str(n).startswith("erased")]
        valid = xx["keeps"]
        pq["total"] = valid.sum(axis=0, dtype="uint16")
        for band in erased_bands:
            clear_name = band.replace("erased", "clear")
            if "aerosol" in band:
                pq[clear_name] = (valid & (~xx[band] & ~xx["erased"])).sum(axis=0, dtype="uint16")
            else:
                pq[clear_name] = (valid & (~xx[band])).sum(axis=0, dtype="uint16")

        if self.aerosol_band and self.aerosol_band == "SR_QA_AEROSOL":
            for aerosol_filter in self.aerosol_filters or []:
                if "closing" in aerosol_filter:
                    erased_band_name = f"erased_{aerosol_filter['closing']:d}_{aerosol_filter['opening']:d}_{aerosol_filter['dilation']:d}"
                    aerosol_band_name = f"clear_{aerosol_filter['closing']:d}_{aerosol_filter['opening']:d}_{aerosol_filter['dilation']:d}_aerosol"
                else:
                    erased_band_name = f"erased_{aerosol_filter['opening']:d}_{aerosol_filter['dilation']:d}"
                    aerosol_band_name = f"clear_{aerosol_filter['opening']:d}_{aerosol_filter['dilation']:d}_aerosol"

                # apply filter on cloud_mask if not exists
                if erased_band_name not in xx:
                    xx[erased_band_name] = mask_cleanup(xx["erased"], aerosol_filter)

                pq[aerosol_band_name] = (valid & (~xx[erased_band_name] & ~xx["erased_aerosol"])).sum(axis=0, dtype="uint16")

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
        cloud_mask = da.bitwise_and(pq_band, 0b0000_0000_0001_1010) != 0  # True=cloud
        keeps = da.bitwise_and(pq_band, 0b0000_0000_0000_0001) == 0  # True=data

        if self.aerosol_band:
            aerosol_band = xx[self.aerosol_band]
            xx = xx.drop_vars([self.aerosol_band])
            # calculate aerosol_level or atmospheric opacity
            if self.aerosol_band == "SR_QA_AEROSOL":
                aerosol_level = da.bitwise_and(aerosol_band, 0b1100_0000) / 64
            elif self.aerosol_band == "SR_ATMOS_OPACITY":
                opacity = (aerosol_band.where(aerosol_band != -9999) * 0.001)

        # drops nodata pixels
        xx = keep_good_only(xx, keeps)

        xx["keeps"] = keeps
        xx["erased"] = cloud_mask
        if self.aerosol_band:
            if self.aerosol_band == "SR_QA_AEROSOL":
                xx["erased_aerosol"] = aerosol_level == 3
            elif self.aerosol_band == "SR_ATMOS_OPACITY":
                xx["erased_aerosol"] = opacity > 0.3

        return xx

    def _fuser(self, xx: xr.Dataset) -> xr.Dataset:
        """
        Fuser cloud and aerosol masking bands with OR
        """
        cloud_mask = xx["erased"]
        xx = xx.drop_vars(["erased"])
        if self.aerosol_band:
            high_aerosol_mask = xx["erased_aerosol"]
            xx = xx.drop_vars(["erased_aerosol"])

        fuser_result = _xr_fuse(xx, partial(_first_valid_np, nodata=0), '')
        fuser_result["erased"] = _xr_fuse(cloud_mask, _fuse_or_np, cloud_mask.name)
        if self.aerosol_band:
            fuser_result["erased_aerosol"] = _xr_fuse(high_aerosol_mask, _fuse_or_np, high_aerosol_mask.name)

        return fuser_result


_plugins.register("pq-ls-bitmask", StatsPQLSBitmask)
