"""
Generic dask helpers
"""

from typing import Tuple, Optional
from random import randint
import numpy as np
from dask.distributed import wait as dask_wait
import dask.array as da
from dask.highlevelgraph import HighLevelGraph
from toolz import partition_all


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


def randomize(prefix: str):
    return '{}-{:08x}'.format(prefix, randint(0, 0xFFFFFFFF))


def _stack_2d_np(shape_in_blocks, *blocks, out=None):
    """
    Stack a bunch of blocks into one plane.

    Takes a flat sequence of blocks in row major order an rearranges them onto a plane.

    Example:
      (2, 3) [a0, a1, a2, a3, a4, a5]
      >>
            [[a0, a1, a2],
             [a3, a4, a5]]

    Blocks should have y,x dimensions first, i.e. a[y,x] or a[y, x, band] or more generally a[y,x,...]
    """
    assert len(blocks) > 0
    assert len(shape_in_blocks) == 2
    assert shape_in_blocks[0]*shape_in_blocks[1] == len(blocks)

    dtype = blocks[0].dtype
    extra_dims = blocks[0].shape[2:]

    h, w = shape_in_blocks

    chunk_y = [b.shape[0] for b in blocks[0:h*w:w]]
    chunk_x = [b.shape[1] for b in blocks[:w]]
    offset = [np.cumsum(x) for x in ([0] + chunk_y, [0] + chunk_x)]
    ny, nx = [x[-1] for x in offset]

    if out is None:
        out = np.empty((ny, nx, *extra_dims), dtype=dtype)
    else:
        pass  # TODO: verify out shape is ok

    for block, idx in zip(blocks, np.ndindex(shape_in_blocks)):
        _y, _x = (offset[i][j] for i, j in zip([0, 1], idx))

        out[_y:_y+block.shape[0],
            _x:_x+block.shape[1]] = block

    return out


def _extract_as_one_block(crop, shape_in_blocks, *blocks):
    out = _stack_2d_np(shape_in_blocks, *blocks)
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


def slice_in_out(s: slice, n: int) -> Tuple[int, int]:
    def fill_if_none(x: Optional[int], val_if_none: int) -> int:
        return val_if_none if x is None else x

    start = fill_if_none(s.start, 0)
    stop = fill_if_none(s.stop, n)
    start, stop = [x if x >= 0 else n+x for x in (start, stop)]
    return (start, stop)


def roi_shape(roi: Tuple[slice, ...], shape: Optional[Tuple[int, ...]] = None) -> Tuple[int, ...]:
    if shape is None:
        # Assume slices are normalised
        return tuple(s.stop - (s.start or 0) for s in roi)

    return tuple(_out - _in
                 for _in, _out in (slice_in_out(s, n)
                                   for s, n in zip(roi, shape)))


def compute_chunk_range(span: slice,
                        chunks: Tuple[int, ...],
                        summed: bool = False) -> Tuple[slice, slice]:
    """
    Compute slice in chunk space and slice after taking just those chunks

    :param span: example: `np.s_[:10]`
    :param chunks: example: xx.chunks[0]

    """
    from bisect import bisect_right, bisect_left

    cs = chunks if summed else tuple(np.cumsum(chunks))
    n = cs[-1]

    _in, _out = slice_in_out(span, n)

    b_start = bisect_right(cs, _in)
    b_end = bisect_left(cs, _out) + 1

    offset = _in - (0 if b_start == 0 else cs[b_start-1])
    sz = _out - _in

    return slice(b_start, b_end), slice(offset, offset+sz)


def extract_dense_2d(xx: da.Array, roi: Tuple[slice, ...], name: str = 'get_roi') -> da.Array:
    """
    xx[roi] -> Dask array with 1 single chunk
    """
    assert len(roi) == xx.ndim
    assert xx.ndim == 2

    broi = []
    crop = []

    for span, chunks in zip(roi, xx.chunks):
        bspan, pspan = compute_chunk_range(span, chunks)
        broi.append(bspan)
        crop.append(pspan)

    broi = tuple(broi)
    crop = tuple(crop)

    xx_chunks = _chunk_getter(xx)
    bshape = roi_shape(broi)

    name = randomize(name)
    dsk = {}
    dsk[(name, 0, 0)] = (_extract_as_one_block, crop, bshape, *xx_chunks(broi))
    dsk = HighLevelGraph.from_collections(name, dsk, dependencies=(xx,))
    shape = roi_shape(crop)
    chunks = tuple((n,) for n in shape)

    return da.Array(dsk, name,
                    chunks=chunks,
                    dtype=xx.dtype,
                    shape=shape)
