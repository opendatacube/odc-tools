"""
Sentinel-2 Geomedian
"""
from typing import Optional, Tuple
import xarray as xr
from odc.stats.model import Task
from odc.algo.io import load_with_native_transform
from odc.algo import erase_bad, geomedian_with_mads, to_rgba
from odc.algo.io import load_enum_filtered
from .model import StatsPluginInterface
from . import _plugins


class StatsGMS2(StatsPluginInterface):
    NAME = "gm_s2_annual"
    SHORT_NAME = NAME
    VERSION = "0.0.0"
    PRODUCT_FAMILY = "geomedian"

    def __init__(
        self,
        resampling: str = "bilinear",
        bands: Optional[Tuple[str, ...]] = None,
        filters: Optional[Tuple[int, int]] = (2, 5),
        work_chunks: Tuple[int, int] = (400, 400),
        mask_band: str = "SCL",
        cloud_classes=(
            "cloud shadows",
            "cloud medium probability",
            "cloud high probability",
            "thin cirrus",
        ),
        basis_band=None,
        nodata_class: Optional[str] = None,
        aux_names=dict(smad="SMAD", emad="EMAD", bcmad="BCMAD", count="COUNT"),
        rgb_bands=None,
        rgb_clamp=(1, 3_000),
    ):
        if bands is None:
            bands = (
                "B02",
                "B03",
                "B04",
                "B05",
                "B06",
                "B07",
                "B08",
                "B8A",
                "B11",
                "B12",
            )
            if rgb_bands is None:
                rgb_bands = ("B04", "B03", "B02")

        self.resampling = resampling
        self.bands = tuple(bands)
        self._basis_band = basis_band or self.bands[0]
        self._renames = aux_names
        self.rgb_bands = rgb_bands
        self.rgb_clamp = rgb_clamp
        self.aux_bands = tuple(
            self._renames.get(k, k) for k in ("smad", "emad", "bcmad", "count")
        )
        self._mask_band = mask_band
        self._nodata_class = nodata_class
        self.filters = filters
        self.cloud_classes = tuple(cloud_classes)
        self._work_chunks = work_chunks

    @property
    def measurements(self) -> Tuple[str, ...]:
        return self.bands + self.aux_bands

    def _native_op_data_band(self, xx):
        from odc.algo import enum_to_bool, keep_good_only

        if not self._mask_band in xx.data_vars:
            return xx

        # Erase Data Pixels for which mask == nodata
        #
        #  xx[mask == nodata] = nodata
        mask = xx[self._mask_band]
        xx = xx.drop_vars([self._mask_band])
        keeps = enum_to_bool(mask, [self._nodata_class], invert=True)
        xx = keep_good_only(xx, keeps)

        return xx

    def input_data(self, task: Task) -> xr.Dataset:
        basis = self._basis_band
        chunks = {"y": -1, "x": -1}
        groupby = "solar_day"

        erased = load_enum_filtered(
            task.datasets,
            self._mask_band,
            task.geobox,
            categories=self.cloud_classes,
            filters=self.filters,
            groupby=groupby,
            resampling=self.resampling,
            chunks={},
        )

        bands_to_load = self.bands
        if self._nodata_class is not None:
            bands_to_load = (*bands_to_load, self._mask_band)

        xx = load_with_native_transform(
            task.datasets,
            bands_to_load,
            task.geobox,
            self._native_op_data_band,
            groupby=groupby,
            basis=basis,
            resampling=self.resampling,
            chunks=chunks,
        )

        xx = erase_bad(xx, erased)
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
            work_chunks=self._work_chunks,
            compute_count=True,
            compute_mads=True,
        )

        gm = geomedian_with_mads(xx, **cfg)
        gm = gm.rename(self._renames)

        return gm

    def rgba(self, xx: xr.Dataset) -> Optional[xr.DataArray]:
        if self.rgb_bands is None:
            return None
        return to_rgba(xx, clamp=self.rgb_clamp, bands=self.rgb_bands)


_plugins.register("gm-s2", StatsGMS2)
