"""
Generic dask helpers
"""

from typing import Tuple, Union, cast
from random import randint
from bisect import bisect_right, bisect_left
import numpy as np
from dask.distributed import wait as dask_wait
import dask.array as da
from dask.highlevelgraph import HighLevelGraph
from toolz import partition_all
from ._tools import ROI, roi_shape, slice_in_out


def chunked_persist(data, n_concurrent, client, verbose=False):
    """Force limited concurrency when persisting a large collection.

       This is useful to control memory usage when operating close to capacity.

       Sometimes `client.persist(data)` will run out of memory, not because
       fully-realized data is large, but because of intermediate data memory
       requirements. This is particularly common when using local dask cluster
       with only one worker.

       This function forces evaluation order of the dask graph to control peak
       memory usage.

       Say you have a largish task graph of 10x10 top-level sub-tasks, you have
       enough memory to process 5 sub-tasks concurrently, but Dask might decide
       to schedule more than that and will cause worker restarts due to out of
       memory errors. With this function you can force dask scheduler to
       persist this collection in batches of 5 concurrent sub-tasks, keeping
       the computation within the memory budget.
    """
    delayed = data.to_delayed().ravel()

    persisted = []
    for chunk in partition_all(n_concurrent, delayed):
        chunk = client.persist(chunk)
        _ = dask_wait(chunk)
        persisted.extend(chunk)
        if verbose:
            print('.', end='')

    # at this point it should be almost no-op
    return client.persist(data)


def randomize(prefix: str) -> str:
    """
    Append random token to name
    """
    return '{}-{:08x}'.format(prefix, randint(0, 0xFFFFFFFF))


def unpack_chunksize(chunk: int, N: int) -> Tuple[int, ...]:
    """
    Compute chunk sizes
    Example: 4, 11 -> (4, 4, 3)
    """
    assert chunk <= N

    nb = N//chunk
    last_chunk = N - chunk*nb
    if last_chunk == 0:
        return tuple(chunk for _ in range(nb))

    return tuple(chunk for _ in range(nb)) + (last_chunk,)


def empty_maker(fill_value, dtype, dsk, name='empty'):
    cache = {}

    def mk_empty(shape: Tuple[int, ...]) -> str:
        x = cache.get(shape, None)
        if x is not None:
            return x

        b_name = name + '_' + "x".join(str(i) for i in shape)
        b_name = randomize(b_name)
        cache[shape] = b_name
        dsk[b_name] = (np.full, shape, fill_value, dtype)
        return b_name

    return mk_empty


def _stack_2d_np(shape_in_blocks, *blocks, out=None, axis=0):
    """
    Stack a bunch of blocks into one plane.

    Takes a flat sequence of blocks in row major order an rearranges them onto a plane.

    Example:
      (2, 3) [a0, a1, a2, a3, a4, a5]
      >>
            [[a0, a1, a2],
             [a3, a4, a5]]

    By default assume that y,x dimensions are first, i.e. a[y,x] or a[y, x, band],
    but one can also stack blocks with extra dimensions by supplying axis= parameter,
    Example: for blocks like this: a[t, y, x, band] use axis=1

    :param shape_in_blocks: (ny, nx) number of blocks
    :param blocks: Blocks in row major order
    :param out: Allows re-use of memory, it must match dtype and output shape exactly
    :param axis: Index of y axis, x axis is then axis+1 (default is axis=0)
    """
    assert len(blocks) > 0
    assert len(shape_in_blocks) == 2
    assert shape_in_blocks[0]*shape_in_blocks[1] == len(blocks)

    dtype = blocks[0].dtype
    bshape = blocks[0].shape
    dims1 = bshape[:axis]
    dims2 = bshape[axis+2:]
    idx1 = tuple(slice(0, None) for _ in range(len(dims1)))
    idx2 = tuple(slice(0, None) for _ in range(len(dims2)))

    h, w = shape_in_blocks

    chunk_y = [b.shape[axis+0] for b in blocks[0:h*w:w]]
    chunk_x = [b.shape[axis+1] for b in blocks[:w]]
    offset = [np.cumsum(x) for x in ([0] + chunk_y, [0] + chunk_x)]
    ny, nx = [x[-1] for x in offset]

    if out is None:
        out = np.empty((*dims1, ny, nx, *dims2), dtype=dtype)
    else:
        pass  # TODO: verify out shape is ok

    for block, idx in zip(blocks, np.ndindex(shape_in_blocks)):
        ny, nx = block.shape[axis:axis+2]
        _y, _x = (offset[i][j] for i, j in zip([0, 1], idx))

        idx = (*idx1,
               slice(_y, _y + ny),
               slice(_x, _x + nx),
               *idx2)

        out[idx] = block

    return out


def _extract_as_one_block(axis, crop, shape_in_blocks, *blocks):
    out = _stack_2d_np(shape_in_blocks, *blocks, axis=axis)
    if crop is None:
        return out
    return out[crop]


def _chunk_getter(xx):
    """
    _chunk_getter(xx)(np.s_[:3, 2:4]) -> (
    (xx.name, 0, 2),
    (xx.name, 0, 3),
    (xx.name, 1, 2),
    ...)
    """
    shape_in_chunks = tuple(map(len, xx.chunks))
    name = xx.name
    xx = np.asarray([{'v': tuple(idx)} for idx in np.ndindex(shape_in_chunks)]).reshape(shape_in_chunks)

    def getter(roi):
        return tuple((name, *x['v']) for x in xx[roi].ravel())

    return getter


def _rechunk_2x2(xx, name='2x2'):
    """
    this is for testing only, ignore it, it's not robust
    """
    assert xx.ndim == 2
    name = randomize(name)
    ny, nx = (len(ch)//2 for ch in xx.chunks[:2])

    dsk = {}
    chunks = _chunk_getter(xx)

    for r, c in np.ndindex((ny, nx)):
        r2 = r*2
        c2 = c*2
        ch_idx = np.s_[r2:r2+2, c2:c2+2]
        _xx = chunks(ch_idx)
        dsk[(name, r, c)] = (_stack_2d_np, (2, 2), *_xx)

    chy = tuple(xx.chunks[0][i*2] + xx.chunks[0][i*2 + 1] for i in range(ny))
    chx = tuple(xx.chunks[1][i*2] + xx.chunks[1][i*2 + 1] for i in range(nx))

    chunks = (chy, chx)
    dsk = HighLevelGraph.from_collections(name, dsk, dependencies=(xx,))

    return da.Array(dsk, name, chunks=chunks, dtype=xx.dtype, shape=xx.shape)


def _compute_chunk_range(span: slice,
                         chunks: Tuple[int, ...],
                         summed: bool = False) -> Tuple[slice, slice]:
    """
    Compute slice in chunk space and slice after taking just those chunks

    :param span: example: `np.s_[:10]`
    :param chunks: example: xx.chunks[0]

    """
    cs = chunks if summed else tuple(np.cumsum(chunks))
    n = cs[-1]

    _in, _out = slice_in_out(span, n)

    b_start = bisect_right(cs, _in)
    b_end = bisect_left(cs, _out) + 1

    offset = _in - (0 if b_start == 0 else cs[b_start-1])
    sz = _out - _in

    return slice(b_start, b_end), slice(offset, offset+sz)


def compute_chunk_range(roi: ROI,
                        chunks: Union[Tuple[int, ...], Tuple[Tuple[int, ...]]],
                        summed: bool = False) -> Tuple[ROI, ROI]:
    """
    Convert ROI in pixels to ROI in blocks (broi) + ROI in pixels (crop) such that

       xx[roi] == stack_blocks(blocks(xx)[broi])[crop]

    Returns
    =======
      broi, crop
    """
    if isinstance(roi, slice):
        chunks = cast(Tuple[int, ...], chunks)
        return _compute_chunk_range(roi, chunks, summed)

    chunks = cast(Tuple[Tuple[int, ...]], chunks)
    assert len(roi) == len(chunks)
    broi = []
    crop = []

    for span, _chunks in zip(roi, chunks):
        bspan, pspan = _compute_chunk_range(span, _chunks)
        broi.append(bspan)
        crop.append(pspan)

    return tuple(broi), tuple(crop)


def crop_2d_dense(xx: da.Array, yx_roi: Tuple[slice, slice], name: str = 'crop_2d', axis: int = 0) -> da.Array:
    """
    xx[.., yx_roi, ..] -> Dask array with 1 single chunk in y,x dimension
    """
    assert len(yx_roi) == 2

    yx_broi, yx_crop = compute_chunk_range(yx_roi, xx.chunks[axis:axis+2])
    assert isinstance(yx_crop, tuple)
    assert isinstance(yx_broi, tuple)

    xx_chunks = _chunk_getter(xx)
    bshape = roi_shape(yx_broi)

    #  tuple(*dims1, y, x, *dims2) -- complete shape in blocks
    dims1 = tuple(map(len, xx.chunks[:axis]))
    dims2 = tuple(map(len, xx.chunks[axis+2:]))

    # Adjust crop to include non-yx dimensions
    crop = tuple(slice(0, None) for _ in dims1) + yx_crop + tuple(slice(0, None) for _ in dims2)

    name = randomize(name)
    dsk = {}
    for ii1 in np.ndindex(dims1):
        roi_ii1 = tuple(slice(i, i+1) for i in ii1)
        for ii2 in np.ndindex(dims2):
            roi_ii2 = tuple(slice(i, i+1) for i in ii2)
            broi = roi_ii1 + yx_broi + roi_ii2
            blocks = xx_chunks(broi)
            assert len(blocks) == bshape[0]*bshape[1]
            dsk[(name, *ii1, 0, 0, *ii2)] = (_extract_as_one_block, axis, crop, bshape, *blocks)

    dsk = HighLevelGraph.from_collections(name, dsk, dependencies=(xx,))
    yx_shape = roi_shape(yx_crop)
    yx_chunks = tuple((n,) for n in yx_shape)
    chunks = xx.chunks[:axis] + yx_chunks + xx.chunks[axis+2:]
    shape = (*xx.shape[:axis], *yx_shape, *xx.shape[axis+2:])

    return da.Array(dsk, name,
                    chunks=chunks,
                    dtype=xx.dtype,
                    shape=shape)
