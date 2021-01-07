from typing import Tuple, Union, Optional, Dict
import numpy as np
import dask.array as da
from distributed import Client
import uuid

ShapeLike = Union[int, Tuple[int, ...]]
DtypeLike = Union[str, np.dtype]
ROI = Union[slice, Tuple[slice, ...]]
MaybeROI = Optional[ROI]

_cache: Dict[str, np.ndarray] = {}


class Cache:
    @staticmethod
    def new(shape: ShapeLike, dtype: DtypeLike) -> str:
        return Cache.put(np.ndarray(shape, dtype=dtype))

    @staticmethod
    def put(x: np.ndarray) -> str:
        k = uuid.uuid4().hex
        _cache[k] = x
        return k

    @staticmethod
    def get(k: str) -> Optional[np.ndarray]:
        return _cache.get(k, None)

    @staticmethod
    def pop(k: str) -> Optional[np.ndarray]:
        return _cache.pop(k, None)


class DataSink:
    def __init__(self, cache_key: str, roi: MaybeROI = None):
        self._k = cache_key
        self._roi = roi

    @staticmethod
    def new(shape: ShapeLike, dtype: DtypeLike) -> "DataSink":
        k = Cache.new(shape, dtype)
        return DataSink(k)

    @staticmethod
    def wrap(x: np.ndarray) -> "DataSink":
        return DataSink(Cache.put(x))

    @property
    def data(self):
        xx = Cache.get(self._k)
        if xx is None:
            return None
        if self._roi is not None:
            xx = xx[self._roi]
        return xx

    def view(self, roi: ROI) -> "DataSink":
        if self._roi is None:
            return DataSink(self._k, roi)
        else:
            raise NotImplementedError("Nested views are not supported yet")

    def unlink(self):
        """This will invalidate this object and all views also"""
        if self._k != "":
            Cache.pop(self._k)
            self._k = ""
            self._roi = None

    def __setitem__(self, key, item):
        self.data[key] = item

    def __getitem__(self, key: ROI) -> "DataSink":
        return self.view(key)


class _YXBTSink:
    def __init__(
        self, cache_key: str, band: Union[int, Tuple[slice, slice, slice, slice]]
    ):
        if isinstance(band, int):
            band = np.s_[:, :, band, :]

        self._k = cache_key
        self._roi = band

    @property
    def data(self):
        xx = Cache.get(self._k)
        if xx is None:
            return None
        return xx[self._roi]

    def __setitem__(self, key, item):
        assert len(key) == 3
        assert item.ndim == 3

        it, iy, ix = key
        self.data[iy, ix, it] = item.transpose([1, 2, 0])


def store_to_mem(
    xx: da.Array, client: Client, out: Optional[np.ndarray] = None
) -> np.ndarray:
    assert client.scheduler.address.startswith("inproc://")
    if out is None:
        sink = DataSink.new(xx.shape, xx.dtype)
    else:
        assert out.shape == xx.shape
        sink = DataSink.wrap(out)

    try:
        fut = da.store(xx, sink, lock=False, compute=False)
        fut = client.compute(fut)
        fut.result()
        return sink.data
    finally:
        sink.unlink()


def yxbt_sink(bands: Tuple[da.Array, ...], client) -> np.ndarray:
    b = bands[0]
    dtype = b.dtype
    nt, ny, nx = b.shape
    nb = len(bands)
    out_key = Cache.new((ny, nx, nb, nt), dtype)
    sinks = [_YXBTSink(out_key, idx) for idx in range(nb)]
    try:
        fut = da.store(bands, sinks, lock=False, compute=False)
        fut = client.compute(fut)
        fut.result()
        return Cache.get(out_key)
    finally:
        Cache.pop(out_key)


def test_cache():
    k = Cache.new((5,), "uint8")
    assert isinstance(k, str)
    xx = Cache.get(k)
    assert xx.shape == (5,)
    assert xx.dtype == "uint8"
    assert Cache.get(k) is xx
    assert Cache.get("some bad key") is None
    assert Cache.pop(k) is xx
    assert Cache.get(k) is None


def test_data_sink():
    import pytest

    ds = DataSink.new((100, 200), "uint16")
    xx = ds.data
    assert xx.shape == (100, 200)
    assert xx.dtype == "uint16"
    assert ds.data is xx

    ds[:] = 0x1020
    assert (xx == 0x1020).all()

    ds2 = ds[:10, :20]
    assert ds2.data.shape == (10, 20)
    ds2[:, :] = 133
    assert (ds.data[:10, :20] == ds2.data).all()
    assert (ds.data[:10, :20] == 133).all()

    with pytest.raises(NotImplementedError):
        ds2.view(np.s_[:3])

    ds.unlink()
