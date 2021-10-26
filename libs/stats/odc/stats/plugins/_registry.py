import pydoc
from typing import Callable, Dict, Type
from functools import partial

from ._base import StatsPluginInterface


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


def register(name: str, plugin_class: Type[StatsPluginInterface]):
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

