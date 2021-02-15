"""
Utilities for unit tests
"""

from datetime import datetime, timedelta
from uuid import UUID
from odc.stats.utils import CompressedDataset


def gen_compressed_dss(n, dt0=datetime(2010, 1, 1, 11, 30, 27), step=timedelta(days=1)):
    if isinstance(step, int):
        step = timedelta(days=step)

    dt = dt0
    for i in range(n):
        yield CompressedDataset(UUID(int=i), dt)
        dt = dt + step
