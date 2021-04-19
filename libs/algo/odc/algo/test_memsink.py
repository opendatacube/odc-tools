import dask
import dask.array as da
import numpy as np
from odc.algo._memsink import (
    Cache,
    CachedArray,
    _da_from_mem,
    da_mem_sink,
    da_yxbt_sink,
    Token,
)


def test_cache():
    k = Cache.new((5,), "uint8")
    assert isinstance(k, Token)
    xx = Cache.get(k)
    assert xx.shape == (5,)
    assert xx.dtype == "uint8"
    assert Cache.get(k) is xx
    assert Cache.get("some bad key") is None
    assert Cache.pop(k) is xx
    assert Cache.get(k) is None


def test_cached_array():
    ds = CachedArray.new((100, 200), "uint16")
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

    ds.release()


def test_da_from_mem():
    shape = (100, 200)
    chunks = (10, 101)
    xx = (np.random.uniform(size=shape) * 1000).astype("uint16")

    k = Cache.put(xx)
    yy = _da_from_mem(
        dask.delayed(str(k)), xx.shape, xx.dtype, chunks=chunks, name="yy"
    )
    assert yy.name.startswith("yy-")
    assert yy.shape == xx.shape
    assert yy.dtype == xx.dtype
    assert yy.chunks[1] == (101, 99)

    assert (yy.compute() == xx).all()

    assert (yy[:3, :5].compute() == xx[:3, :5]).all()


def test_cache_dask_new():
    tk = Cache.dask_new((10, 10), "float32", "jj")
    assert dask.is_dask_collection(tk)
    assert tk.key.startswith("jj-")


def test_da_to_mem():
    xx = da.random.uniform(size=(10, 20), chunks=(5, 4))
    yy = da_mem_sink(xx, chunks=(-1, -1), name="yy")

    assert dask.is_dask_collection(yy)
    assert xx.shape == yy.shape
    assert xx.dtype == yy.dtype

    _yy = yy.compute()
    _xx = xx.compute()
    assert (_xx == _yy).all()


def test_yxbt_sink():
    NT, NY, NX = 3, 10, 20
    NB = 2
    aa = da.random.uniform(size=(NT, NY, NX), chunks=(1, 5, 4))
    bb = da.random.uniform(size=(NT, NY, NX), chunks=(1, 5, 4))

    yxbt = da_yxbt_sink((aa, bb), (5, 5, -1, -1))
    assert yxbt.chunksize == (5, 5, NB, NT)
    assert yxbt.shape == (NY, NX, NB, NT)
    assert yxbt.dtype == aa.dtype

    _yxbt = yxbt.compute()
    _aa = aa.compute()
    _bb = bb.compute()
    for t_idx in range(NT):
        assert (_yxbt[:, :, 0, t_idx] == _aa[t_idx]).all()
        assert (_yxbt[:, :, 1, t_idx] == _bb[t_idx]).all()
