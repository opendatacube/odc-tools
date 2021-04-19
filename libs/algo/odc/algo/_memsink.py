from typing import Any, Dict, Optional, Tuple, Union, Hashable
import numpy as np
import xarray as xr
import dask
import dask.array as da
from dask.delayed import Delayed
from dask.base import tokenize
from dask.highlevelgraph import HighLevelGraph
from distributed import Client
import uuid
from ._dask import unpack_chunks, _roi_from_chunks


ShapeLike = Union[int, Tuple[int, ...]]
DtypeLike = Union[str, np.dtype]
ROI = Union[slice, Tuple[slice, ...]]
MaybeROI = Optional[ROI]
CacheKey = Union["Token", str]

_cache: Dict[str, np.ndarray] = {}


class Token:
    __slots__ = ["_k"]

    def __init__(self, k: str):
        # print(f"Token.init(<{k}>)@0x{id(self):08X}")
        self._k = k

    def __str__(self) -> str:
        return self._k

    def __bool__(self):
        return len(self._k) > 0

    def release(self):
        if self:
            Cache.pop(self)
            self._k = ""

    def __del__(self):
        # print(f"Token.del(<{self._k}>)@0x{id(self):08X}")
        self.release()

    def __getstate__(self):
        print(f"Token.__getstate__() <{self._k}>@0x{id(self):08X}")
        raise ValueError("Token should not be pickled")

    def __setstate__(self, k):
        print(f"Token.__setstate__(<{k}>)@0x{id(self):08X}")
        raise ValueError("Token should not be pickled")


class Cache:
    @staticmethod
    def new(shape: ShapeLike, dtype: DtypeLike) -> Token:
        return Cache.put(np.ndarray(shape, dtype=dtype))

    @staticmethod
    def dask_new(shape: ShapeLike, dtype: DtypeLike, name: str = "") -> Delayed:
        if name == "":
            name = f"mem_array_{str(dtype)}"

        name = name + "-" + tokenize(name, shape, dtype)
        dsk = {name: (Cache.new, shape, dtype)}
        return Delayed(name, dsk)

    @staticmethod
    def put(x: np.ndarray) -> Token:
        k = uuid.uuid4().hex
        _cache[k] = x
        return Token(k)

    @staticmethod
    def get(k: CacheKey) -> Optional[np.ndarray]:
        return _cache.get(str(k), None)

    @staticmethod
    def pop(k: CacheKey) -> Optional[np.ndarray]:
        return _cache.pop(str(k), None)


class CachedArray:
    def __init__(self, token_or_key: CacheKey):
        self._tk = token_or_key

    @property
    def data(self) -> np.ndarray:
        xx = Cache.get(self._tk)
        if xx is None:
            raise ValueError("Source array is missing from cache")
        return xx

    @property
    def shape(self) -> Tuple[int, ...]:
        return self.data.shape

    @property
    def dtype(self):
        return self.data.dtype

    @property
    def ndim(self):
        return self.data.ndim

    def __getitem__(self, key: ROI) -> np.ndarray:
        return self.data[key]

    def __setitem__(self, key, item):
        self.data[key] = item

    @staticmethod
    def new(shape: ShapeLike, dtype: DtypeLike) -> "CachedArray":
        return CachedArray(Cache.new(shape, dtype))

    @staticmethod
    def wrap(x: np.ndarray) -> "CachedArray":
        return CachedArray(Cache.put(x))

    def release(self) -> Optional[np.ndarray]:
        return Cache.pop(self._tk)


class _YXBTSink:
    def __init__(
        self,
        token_or_key: CacheKey,
        band: Union[int, Tuple[slice, slice, slice, slice]],
    ):
        if isinstance(band, int):
            band = np.s_[:, :, band, :]

        self._tk = token_or_key
        self._roi = band

    @property
    def data(self):
        xx = Cache.get(self._tk)
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
    token = None
    if out is None:
        sink = dask.delayed(CachedArray.new)(xx.shape, xx.dtype)
    else:
        assert out.shape == xx.shape
        token = Cache.put(out)
        sink = dask.delayed(CachedArray)(str(token))

    try:
        fut = da.store(xx, sink, lock=False, compute=False)
        fut, _sink = client.compute([fut, sink])
        fut.result()
        return _sink.result().data
    finally:
        if token is not None:
            token.release()


def yxbt_sink_to_mem(bands: Tuple[da.Array, ...], client: Client) -> np.ndarray:
    assert client.scheduler.address.startswith("inproc://")

    b = bands[0]
    dtype = b.dtype
    nt, ny, nx = b.shape
    nb = len(bands)
    token = Cache.new((ny, nx, nb, nt), dtype)
    sinks = [_YXBTSink(str(token), idx) for idx in range(nb)]
    try:
        fut = da.store(bands, sinks, lock=False, compute=False)
        fut = client.compute(fut)
        fut.result()
        return Cache.get(token)
    finally:
        token.release()


def _chunk_extractor(cache_key: CacheKey, roi: ROI, *deps) -> np.ndarray:
    src = Cache.get(cache_key)
    assert src is not None
    return src[roi]


def _da_from_mem(
    token: Delayed,
    shape: ShapeLike,
    dtype: DtypeLike,
    chunks: Tuple[int, ...],
    name: str = "from_mem",
) -> da.Array:
    """
    Construct dask view of some yet to be computed in RAM store.

    :param token: Should evaluate to either Token or string key in to the Cache,
                  which is expected to contain ``numpy`` array of supplied
                  ``shape`` and ``dtype``

    :param shape: Expected shape of the future array

    :param dtype: Expected dtype of the future array

    :param chunks: Tuple of integers describing chunk partitioning for output array

    :param name: Dask name

    Gotchas
    =======

    - Output array can not be moved from one worker to another.
      - Works with in-process Client
      - Works with single worker cluster
      - Can work if scheduler is told to schedule this on a single worker

    - Cache life cycle management can be tough. If token evaluates to a
      ``Token`` object then automatic cache cleanup should happen when output
      array is destroyed. If it is just a string, then it's up to caller to
      ensure that there is cleanup and no use after free.

    Returns
    =======
    Dask Array
    """
    if not isinstance(shape, tuple):
        shape = (shape,)

    assert dask.is_dask_collection(token)
    assert len(shape) == len(chunks)

    _chunks = unpack_chunks(chunks, shape)
    _rois = [tuple(_roi_from_chunks(ch)) for ch in _chunks]
    _roi = lambda idx: tuple(_rois[i][k] for i, k in enumerate(idx))

    shape_in_chunks = tuple(len(ch) for ch in _chunks)

    dsk = {}
    name = name + "-" + tokenize(token, shape, dtype, chunks)
    dsk[name] = []

    for idx in np.ndindex(shape_in_chunks):
        dsk[(name, *idx)] = (_chunk_extractor, token.key, _roi(idx))
        dsk[name].append((name, *idx))

    dsk = HighLevelGraph.from_collections(name, dsk, dependencies=[token])

    return da.Array(dsk, name, shape=shape, dtype=dtype, chunks=_chunks)


def da_mem_sink(xx: da.Array, chunks: Tuple[int, ...], name="memsink") -> da.Array:
    """
    It's a kind of fancy rechunk for special needs.

    Assumptions
    - Single worker only
    - ``xx`` can fit in RAM of the worker

    Note that every output chunk depends on ALL of input chunks.

    On some Dask worker:
    - Fully evaluate ``xx`` and serialize to RAM
    - Present in RAM view of the result with a different chunking regime

    A common use case would be to load a large collection (>50% of RAM) that
    needs to be processed by some non-Dask code as a whole. A simple
    ``do_stuff(xx.compute())`` would not work as duplicating RAM is not an
    option in that scenario. Normal rechunk might also run out of RAM and
    introduces large memory copy overhead as all input chunks need to be cached
    then re-assembled into a different chunking structure.
    """
    tk = tokenize(xx)

    token = Cache.dask_new(xx.shape, xx.dtype, f"{name}_alloc")

    # Store everything to MEM and only then evaluate to Token
    sink = dask.delayed(CachedArray)(token)
    fut = da.store(xx, sink, lock=False, compute=False)
    sink_name = f"{name}_collect-{tk}"
    dsk = dict(fut.dask)
    dsk[sink_name] = (lambda *x: x[0], token.key, *fut.dask[fut.key])
    dsk = HighLevelGraph.from_collections(sink_name, dsk, dependencies=[sink])
    token_done = Delayed(sink_name, dsk)

    return _da_from_mem(
        token_done, shape=xx.shape, dtype=xx.dtype, chunks=chunks, name=name
    )


def da_yxbt_sink(
    bands: Tuple[da.Array, ...], chunks: Tuple[int, ...], name="yxbt"
) -> da.Array:
    """
    each band is in <t,y,x>
    output is <y,x,b,t>

    eval(bands) |> transpose(YXBT) |> Store(RAM) |> DaskArray(RAM, chunks)
    """
    tk = tokenize(*bands)

    b = bands[0]
    dtype = b.dtype
    nt, ny, nx = b.shape
    nb = len(bands)
    shape = (ny, nx, nb, nt)

    token = Cache.dask_new(shape, dtype, f"{name}_alloc")

    sinks = [dask.delayed(_YXBTSink)(token, idx) for idx in range(nb)]
    fut = da.store(bands, sinks, lock=False, compute=False)
    sink_name = f"{name}_collect-{tk}"
    dsk = dict(fut.dask)
    dsk[sink_name] = (lambda *x: x[0], token.key, *fut.dask[fut.key])
    dsk = HighLevelGraph.from_collections(sink_name, dsk, dependencies=sinks)
    token_done = Delayed(sink_name, dsk)

    return _da_from_mem(token_done, shape=shape, dtype=dtype, chunks=chunks, name=name)


def yxbt_sink(ds: xr.Dataset, chunks: Tuple[int, int, int, int]) -> xr.DataArray:
    """
    Given a Dask dataset with several bands and ``T,Y,X`` axis order on input,
    turn that into a Dask DataArray with axis order being ``Y, X, Band, T``.

    The way this function work is
    - Evaluate all input data before making any output chunk available for further processing
    - For each input block store it into appropriate location in RAM.
    - Expose in RAM store as Dask Array with requested chunking regime

    This is used for Geomedian computation mostly, for GM chunks need to be ``(ny, nx, -1,-1)``.

    :param ds: Dataset with Dask based arrays ``T,Y,X`` axis order
    :param chunks: Chunk size for output array, example: ``(100, 100, -1, -1)``

    Gotchas
    =======

    - Output array can not be moved from one worker to another.
      - Works with in-process Client
      - Works with single worker cluster
      - Can work if scheduler is told to schedule this on a single worker


    Returns
    =======
    xarray DataArray backed by Dask array.
    """
    b0, *_ = ds.data_vars.values()
    data = da_yxbt_sink(tuple(dv.data for dv in ds.data_vars.values()), chunks)
    attrs = dict(b0.attrs)
    dims = b0.dims[1:] + ("band", b0.dims[0])

    coords: Dict[Hashable, Any] = {k: c for k, c in ds.coords.items()}
    coords["band"] = list(ds.data_vars)

    return xr.DataArray(data=data, dims=dims, coords=coords, attrs=attrs)
