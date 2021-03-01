"""
Utilities for unit tests
"""
from datetime import datetime, timedelta
import time
from uuid import UUID
import xarray as xr
import dask.array as da
import numpy as np
from odc.stats.utils import CompressedDataset
from odc.stats.model import StatsPluginInterface


class DummyPlugin(StatsPluginInterface):
    NAME = "test_long"
    SHORT_NAME = "test_short"
    VERSION = "1.2.3"
    PRODUCT_FAMILY = "test"

    def __init__(self, bands=("a", "b", "c"), delay=0):
        self._bands = tuple(bands)
        self._delay = delay

    @property
    def measurements(self):
        return self._bands

    def input_data(self, task):
        ts = sorted([ds.center_time for ds in task.datasets])
        xx = mk_dask_xx(
            task.geobox, timestamps=ts, mode="random", attrs=dict(nodata=-9999)
        )
        return xr.Dataset(dict(xx=xx))

    def _delayed_add_op(self, data, offset):
        if self._delay > 0:
            # don't sleep when called in construction stages
            if data.shape[0] > 1:
                time.sleep(self._delay)

        return data + offset

    def _add(self, x, offset):
        data = da.map_blocks(self._delayed_add_op, x.data, offset, dtype=x.dtype)
        return xr.DataArray(data=data, coords=x.coords, dims=x.dims, attrs=x.attrs)

    def reduce(self, xx):
        _xx = xx.isel(time=0).xx
        bands = {band: self._add(_xx, idx) for idx, band in enumerate(self._bands)}
        return xr.Dataset(bands)


def gen_compressed_dss(n, dt0=datetime(2010, 1, 1, 11, 30, 27), step=timedelta(days=1)):
    if isinstance(step, int):
        step = timedelta(days=step)

    dt = dt0
    for i in range(n):
        yield CompressedDataset(UUID(int=i), dt)
        dt = dt + step


def mk_time_coords(timestamps):
    data = np.asarray(timestamps, dtype="datetime64[ns]")
    return xr.DataArray(data=data, coords={"time": data}, dims=("time",), name="time")


def mk_dask_xx(
    geobox,
    chunks={"x": -1, "y": -1},
    dtype="uint16",
    timestamps=None,
    attrs={},
    mode="random",
):
    if timestamps is None:
        timestamps = [datetime.utcnow()]

    dtype = np.dtype(dtype)
    _chunks = (chunks.get("time", 1), chunks.get("y", -1), chunks.get("x", -1))
    shape = (len(timestamps),) + geobox.shape
    if mode == "zeros":
        data = da.zeros(shape, dtype=dtype, chunks=_chunks)
    elif mode == "ones":
        data = da.ones(shape, dtype=dtype, chunks=_chunks)
    elif mode == "random":
        data = da.random.uniform(0, 1, size=shape, chunks=_chunks)
        if dtype.kind != "f":
            data = (data * 100).astype(dtype)
        elif data.dtype != dtype:
            data = data.astype(dtype)

    coords = geobox.xr_coords(with_crs=True)
    coords["time"] = mk_time_coords(timestamps)

    return xr.DataArray(data=data, dims=("time", "y", "x"), coords=coords, attrs=attrs)
