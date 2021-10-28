from abc import ABC, abstractmethod
from typing import Callable, Mapping, Optional, Sequence, Tuple

import xarray as xr
from datacube.model import Dataset
from datacube.utils.geometry import GeoBox
from odc.algo import to_rgba
from odc.algo.io import load_with_native_transform


class StatsPluginInterface(ABC):
    NAME = "*unset*"
    SHORT_NAME = ""
    VERSION = "0.0.0"
    PRODUCT_FAMILY = "statistics"

    def __init__(self,
                 resampling: str = "bilinear",
                 input_bands: Sequence[str] = [],
                 chunks: Mapping[str, int] = {"y": -1, "x": -1},
                 basis: Optional[str] = None,
                 group_by: str = "solar_day",
                 rgb_bands: Optional[Sequence[str]] = None,
                 rgb_clamp: Tuple[float, float] = (1.0, 3_000.0)
                ):
        self.resampling = resampling
        self.input_bands = input_bands
        self.chunks = chunks
        self.basis = basis
        self.group_by = group_by
        self.rgb_bands = rgb_bands
        self.rgb_clamp = rgb_clamp

    @property
    @abstractmethod
    def measurements(self) -> Tuple[str, ...]:
        pass

    def native_transform(self, xx: xr.Dataset) -> xr.Dataset:
        return xx

    def fuser(self, xx: xr.Dataset) -> xr.Dataset:
        return xx

    def input_data(self, datasets: Sequence[Dataset], geobox: GeoBox) -> xr.Dataset:
        xx = load_with_native_transform(
            datasets,
            bands=self.input_bands,
            geobox=geobox,
            native_transform=self.native_transform,
            basis=self.basis,
            groupby=self.group_by,
            fuser=self.fuser,
            resampling=self.resampling,
            chunks=self.chunks,
        )
        return xx

    @abstractmethod
    def reduce(self, xx: xr.Dataset) -> xr.Dataset:
        pass

    def rgba(self, xx: xr.Dataset) -> Optional[xr.DataArray]:
        """
        Given result of ``.reduce(..)`` optionally produce RGBA preview image
        """
        if self.rgb_bands is None:
            return None
        return to_rgba(xx, clamp=self.rgb_clamp, bands=self.reg_bands)
