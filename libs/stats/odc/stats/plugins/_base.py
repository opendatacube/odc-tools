from abc import ABC, abstractmethod
from typing import Tuple, Sequence, Optional

import xarray as xr
from datacube.model import Dataset
from datacube.utils.geometry import GeoBox
from odc.algo import to_rgba


class StatsPluginInterface(ABC):
    NAME = "*unset*"
    SHORT_NAME = ""
    VERSION = "0.0.0"
    PRODUCT_FAMILY = "statistics"

    def __init__(self, rgb_bands=None, rgb_clamp=[1, 3_000]):
        self.rgb_bands = rgb_bands
        self.rgb_clamp = rgb_clamp

    @property
    @abstractmethod
    def measurements(self) -> Tuple[str, ...]:
        pass

    @abstractmethod
    def input_data(self, datasets: Sequence[Dataset], geobox: GeoBox) -> xr.Dataset:
        pass

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
