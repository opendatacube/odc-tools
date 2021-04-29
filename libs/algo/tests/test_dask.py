import pytest
import numpy as np
import dask.array as da
from dask import delayed
from dask.distributed import Client
import toolz
from odc.algo._dask import (
    _rechunk_2x2,
    _stack_2d_np,
    compute_chunk_range,
    crop_2d_dense,
    unpack_chunksize,
    wait_for_future,
)


@delayed
def slow_compute(delay, value=None, fail=False):
    import time

    if delay > 0:
        time.sleep(delay)
    if fail:
        raise ValueError("Failing as requested")
    return value


def test_wait_for_future():
    client = Client(
        processes=False, n_workers=1, threads_per_worker=1, dashboard_address=None
    )
    fut = client.compute(slow_compute(1))
    rr = list(wait_for_future(fut, 0.1))
    assert fut.done()
    assert len(rr) > 1

    # Check that exception doesn't leak out
    fut = client.compute(slow_compute(1, fail=True))
    rr = list(wait_for_future(fut, 0.1))
    assert fut.done()
    assert fut.status == "error"
    assert len(rr) > 1
    print(fut)


def test_1():
    xx = da.random.uniform(0, 10, size=(16, 6), chunks=(4, 3)).astype("uint8")
    yy = _rechunk_2x2(xx)
    assert xx.dtype == yy.dtype
    assert xx.shape == yy.shape
    assert (xx.compute() == yy.compute()).all()


@pytest.mark.parametrize(
    "chunk, n, expect",
    [
        (4, 7, (4, 3)),
        (3, 9, (3, 3, 3)),
        (8, 8, (8,)),
        (1, 3, (1, 1, 1)),
        (10, 3, (3,)),
    ],
)
def test_unpack_chunks(chunk, n, expect):
    assert unpack_chunksize(chunk, n) == expect


@pytest.mark.parametrize(
    "shape, block_shape",
    [
        [(2, 3), (2, 2)],
        [(3, 2), (1, 2)],
        [(1, 2), (2, 3)],
        [(1, 1), (2, 3)],
        [(2, 3), (2, 3, 3)],
        [(2, 3), (3, 2, 4)],
        [(2, 3), (3, 2, 4, 1)],
    ],
)
def test_stack2d_np(shape, block_shape, verbose=False):
    aa = np.zeros((block_shape), dtype="int8")

    h, w = shape
    seq = [aa + i for i in range(w * h)]

    expect = np.vstack([np.hstack(row) for row in toolz.partition_all(w, seq)])

    cc = _stack_2d_np(shape, *seq)

    assert (cc == expect).all()

    if verbose:
        print()
        if cc.ndim == 2:
            print(cc)
        elif cc.ndim == 3:
            print(cc[:, :, 0], f"x{cc.shape[2:]}")
        else:
            print(f"x{cc.shape}")


def test_stack2d_np_ndim(verbose=False):
    shape = (4, 3)
    h, w = shape

    aa = np.zeros((10, 2, 3, 3), dtype="int8")
    seq = [aa + i for i in range(w * h)]

    cc = _stack_2d_np(shape, *seq, axis=1)
    assert cc.shape == (10, 8, 9, 3)
    if verbose:
        print()
        print(cc[0, :, :, 0])


@pytest.mark.parametrize(
    "span, chunks, summed, bspan, pspan",
    [
        (np.s_[:], (4, 4), False, slice(0, 2), slice(0, 8)),
        (np.s_[0:], (4, 4), False, slice(0, 2), slice(0, 8)),
        (np.s_[0:-1], (4, 4), False, slice(0, 2), slice(0, 7)),
        (np.s_[-1:], (4, 4), False, slice(1, 2), slice(3, 4)),
        (np.s_[-4:], (4, 4), False, slice(1, 2), slice(0, 4)),
        (np.s_[0:8], (4, 4), False, slice(0, 2), slice(0, 8)),
        (np.s_[1:], (4, 4), False, slice(0, 2), slice(1, 8)),
        (np.s_[1:4], (4, 4), False, slice(0, 1), slice(1, 4)),
        (np.s_[:], (2, 4, 6, 11, 13), True, slice(0, 5), slice(0, 13)),
        (np.s_[2:7], (2, 4, 6, 11, 13), True, slice(1, 4), slice(0, 5)),
        (np.s_[3:7], (2, 4, 6, 11, 13), True, slice(1, 4), slice(1, 5)),
        (np.s_[3:], (2, 4, 6, 11, 13), True, slice(1, 5), slice(1, 13 - 3 + 1)),
    ],
)
def test_chunk_range(span, chunks, summed, bspan, pspan):
    _bspan, _pspan = compute_chunk_range(span, chunks, summed)
    assert _bspan == bspan
    assert _pspan == pspan


@pytest.mark.parametrize(
    "yx_roi",
    [np.s_[:, :], np.s_[:1, :1], np.s_[3:, 1:], np.s_[3:-3, 1:-5], np.s_[3:-3, -5:]],
)
def test_crop_2d_dense(yx_roi):
    # Y,X
    xx = da.random.uniform(0, 10, size=(16, 6), chunks=(4, 3)).astype("uint8")

    yy = crop_2d_dense(xx, yx_roi)
    assert xx.dtype == yy.dtype
    assert yy.shape == xx[yx_roi].shape
    assert yy.shape == yy.chunksize

    assert (xx[yx_roi].compute() == yy.compute()).all()

    # Y, X, Band
    xx = da.random.uniform(0, 10, size=(16, 6, 4), chunks=(4, 3, 4)).astype("uint8")

    yy = crop_2d_dense(xx, yx_roi)
    _roi = (*yx_roi, np.s_[:])
    assert xx.dtype == yy.dtype
    assert yy.shape == xx[_roi].shape
    assert yy.shape[:2] == yy.chunksize[:2]

    assert (xx[_roi].compute() == yy.compute()).all()

    # Time, Y, X
    xx = da.random.uniform(0, 10, size=(5, 16, 6), chunks=(1, 4, 3)).astype("uint8")

    yy = crop_2d_dense(xx, yx_roi, axis=1)
    _roi = (np.s_[:], *yx_roi)
    assert xx.dtype == yy.dtype
    assert yy.shape == xx[_roi].shape
    assert yy.shape[1:3] == yy.chunksize[1:3]
    assert yy.chunksize[0] == xx.chunksize[0]

    assert (xx[_roi].compute() == yy.compute()).all()

    # Time, Y, X, Band
    xx = da.random.uniform(0, 10, size=(5, 16, 6, 3), chunks=(1, 4, 3, 3)).astype(
        "uint8"
    )

    yy = crop_2d_dense(xx, yx_roi, axis=1)
    _roi = (np.s_[:], *yx_roi, np.s_[:])
    assert xx.dtype == yy.dtype
    assert yy.shape == xx[_roi].shape
    assert yy.shape[1:3] == yy.chunksize[1:3]
    assert yy.chunksize[0] == xx.chunksize[0]
    assert yy.chunksize[-1] == xx.chunksize[-1]

    assert (xx[_roi].compute() == yy.compute()).all()
