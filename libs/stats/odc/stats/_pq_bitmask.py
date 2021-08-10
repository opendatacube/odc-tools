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

filters = dict containing band-name as key and list of iterable tuples of morphological operations as it's value.
    Provide morphological operations in the order you want them to perform.
    For example,

    filters = {
        "clear_10_2_2": [("closing", 10), ("opening", 2), ("dilation", 2)]
    }

aerosol_filters = dict containing band-name as key and list of iterable tuples of morphological operations as it's value.
    Provide morphological operations in the order you want them to perform.
    Similar to filters but for aerosol.
    For example,

    aerosol_filters = {
        "clear_10_2_2_aerosol": [("closing", 10), ("opening", 2), ("dilation", 2)]
    }

resampling = "nearest"
"""

from functools import partial
from typing import Any, Dict, List, Optional, Tuple, Iterable

import dask.array as da
import xarray as xr
from datacube.utils import masking
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
            # provide flags with high cloud bits definition
            flags: Dict[str, Optional[Any]] = dict(
                cloud="high_confidence",
                cirrus="high_confidence",
            ),
            nodata_flags: Dict[str, Optional[Any]] = dict(nodata=False),
            filters: Optional[Dict[str, Iterable[Tuple[str, int]]]] = None,
            aerosol_filters: Optional[Dict[str, Iterable[Tuple[str, int]]]] = None,
            resampling: str = "nearest",
    ):
        self.pq_band = pq_band
        self.aerosol_band = aerosol_band
        self.flags = flags
        self.nodata_flags = nodata_flags
        self.filters = filters or {}
        self.aerosol_filters = aerosol_filters or {}
        self.resampling = resampling

    @property
    def measurements(self) -> Tuple[str, ...]:
        """
        Output product measurements
        """
        measurements = [
            "total",
            "clear",
            *list(self.filters)
        ]

        if self.aerosol_band:
            measurements.append("clear_aerosol")
            if self.aerosol_band == "SR_QA_AEROSOL":
                measurements.extend(list(self.aerosol_filters))

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

        for band, mask_filters in self.filters.items():
            erased_filter_band_name = band.replace("clear", "erased")
            xx[erased_filter_band_name] = mask_cleanup(xx["erased"], mask_filters=mask_filters)

        erased_bands = [str(n) for n in xx.data_vars if str(n).startswith("erased")]
        valid = xx["keeps"]
        pq["total"] = valid.sum(axis=0, dtype="uint16")
        for band in erased_bands:
            clear_band_name = band.replace("erased", "clear")
            if "aerosol" in band:
                pq[clear_band_name] = (valid & (~xx[band] & ~xx["erased"])).sum(axis=0, dtype="uint16")
            else:
                pq[clear_band_name] = (valid & (~xx[band])).sum(axis=0, dtype="uint16")

        if self.aerosol_band and self.aerosol_band == "SR_QA_AEROSOL":
            for band, mask_filters in self.aerosol_filters.items():
                erased_aerosol_filter_band_name = band.replace("clear", "erased")
                if erased_aerosol_filter_band_name not in xx:
                    xx[erased_aerosol_filter_band_name] = mask_cleanup(xx["erased"], mask_filters=mask_filters)

                pq[band] = (valid & (~xx[erased_aerosol_filter_band_name] & ~xx["erased_aerosol"])).sum(axis=0, dtype="uint16")

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

        flags_def = masking.get_flags_def(pq_band)

        # set cloud_mask - True=cloud, False=non-cloud
        mask, _ = masking.create_mask_value(flags_def, **self.flags)
        cloud_mask = (pq_band & mask) != 0

        # set no_data bitmask - True=data, False=no-data
        nodata_mask, _ = masking.create_mask_value(flags_def, **self.nodata_flags)
        keeps = (pq_band & nodata_mask) == 0

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
