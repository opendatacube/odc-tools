from typing import Callable, Dict
from functools import partial

from .model import StatsPluginInterface

PluginFactory = Callable[..., StatsPluginInterface]

_plugins: Dict[str, PluginFactory] = {}


def _new(plugin_class, *args, **kwargs) -> StatsPluginInterface:
    return plugin_class(*args, **kwargs)


def resolve(name: str) -> PluginFactory:
    maker = _plugins.get(name)
    if maker is None:
        raise ValueError(f"Failed to resolved named plugin: '{name}'")
    return maker


def register(name: str, plugin_class):
    _plugins[name] = partial(_new, plugin_class)
