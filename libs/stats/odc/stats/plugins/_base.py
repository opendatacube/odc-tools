import pydoc
from abc import ABC, abstractmethod
from typing import Callable, Dict, Tuple, Sequence, Optional
from functools import partial

import xarray as xr
from datacube.model import Dataset
from datacube.utils.geometry import GeoBox

class StatsPluginInterface(ABC):
    NAME = "*unset*"
    SHORT_NAME = ""
    VERSION = "0.0.0"
    PRODUCT_FAMILY = "statistics"

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
        return None


PluginFactory = Callable[..., StatsPluginInterface]

_plugins: Dict[str, PluginFactory] = {}


def _new(plugin_class, *args, **kwargs) -> StatsPluginInterface:
    return plugin_class(*args, **kwargs)


def resolve(name: str) -> PluginFactory:
    maker = _plugins.get(name)
    if maker is None:
        maker = pydoc.locate(name)
        if maker is not None:
            if not issubclass(maker, (StatsPluginInterface,)):
                raise ValueError(f"Custom StatsPlugin '{name}' is not derived from StatsPluginInterface")
            return partial(_new, maker)

    if maker is None:
        raise ValueError(f"Failed to resolved named plugin: '{name}'")
    return maker


def register(name: str, plugin_class):
    _plugins[name] = partial(_new, plugin_class)


def import_all():
    import importlib

    # TODO: make that more automatic
    modules = [
        "odc.stats.plugins.fc_percentiles",
        "odc.stats.plugins.tcw_percentiles",
        "odc.stats.plugins.gm",
        "odc.stats.plugins.gm_ls_bitmask",
        "odc.stats.plugins.pq",
        "odc.stats.plugins.pq_bitmask",
        "odc.stats.plugins.wofs",
    ]

    for mod in modules:
        try:
            importlib.import_module(mod)
        except ModuleNotFoundError:
            pass

