"""
Utilities for unit tests
"""

from datetime import datetime, timedelta
from uuid import UUID
from odc.stats.utils import CompressedDataset
from odc.stats.model import StatsPluginInterface


class DummyPlugin(StatsPluginInterface):
    NAME = "test_long"
    SHORT_NAME = "test_short"
    VERSION = "1.2.3"
    PRODUCT_FAMILY = "test"

    def __init__(self, bands=("a", "b", "c")):
        self._bands = tuple(bands)

    @property
    def measurements(self):
        return self._bands

    def input_data(self, task):
        return None

    def reduce(self, xx):
        return xx


def gen_compressed_dss(n, dt0=datetime(2010, 1, 1, 11, 30, 27), step=timedelta(days=1)):
    if isinstance(step, int):
        step = timedelta(days=step)

    dt = dt0
    for i in range(n):
        yield CompressedDataset(UUID(int=i), dt)
        dt = dt + step
