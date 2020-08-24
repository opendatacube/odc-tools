"""
Sentinel 2 pixel quality stats
"""
import xarray as xr

from odc.stats.model import Task
from odc.algo.io import load_with_native_transform
from odc.algo import enum_to_bool

bad_pixel_classes = (0, 'saturated or defective')
cloud_classes = ('cloud shadows',
                 'cloud medium probability',
                 'cloud high probability',
                 'thin cirrus')


def _pq_native_transform(xx):
    """
    config:
    bad_pixel_classes
    cloud_classes
    """

    valid = enum_to_bool(xx.SCL, bad_pixel_classes, invert=True, value_true=255, dtype='uint8')
    clear = enum_to_bool(xx.SCL, bad_pixel_classes+cloud_classes, invert=True, value_true=255, dtype='uint8')
    return xr.Dataset(dict(valid=valid, clear=clear))


def pq_input_data(task: Task, resampling: str) -> xr.Dataset:
    """
    .valid  Bool
    .clear  Bool
    """
    xx = load_with_native_transform(task.datasets,
                                    ['SCL'],
                                    task.geobox,
                                    _pq_native_transform,
                                    groupby='solar_day',
                                    resampling=resampling,
                                    chunks={'x': 4800, 'y': 4800})
    return xx > 127


def pq_reduce(xx: xr.Dataset) -> xr.Dataset:
    """
    """
    pq = xr.Dataset(dict(clear=xx.clear.sum(axis=0, dtype='uint16'),
                         total=xx.valid.sum(axis=0, dtype='uint16')))

    return pq
