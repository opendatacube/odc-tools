import pytest
import numpy as np
import dask.array as da
import toolz
from ._dask import _rechunk_2x2, _stack_2d_np, compute_chunk_range, crop_2d_dense


def test_1():
    xx = da.random.uniform(0, 10, size=(16, 6),
                           chunks=(4, 3)).astype('uint8')
    yy = _rechunk_2x2(xx)
    assert xx.dtype == yy.dtype
    assert xx.shape == yy.shape
    assert (xx.compute() == yy.compute()).all()


@pytest.mark.parametrize("shape, block_shape", [
    [(2, 3), (2, 2)],
    [(3, 2), (1, 2)],
    [(1, 2), (2, 3)],
    [(1, 1), (2, 3)],
    [(2, 3), (2, 3, 3)],
    [(2, 3), (3, 2, 4)],
    [(2, 3), (3, 2, 4, 1)],
])
def test_stack2d_np(shape, block_shape, verbose=False):
    aa = np.zeros((block_shape), dtype='int8')

    h, w = shape
    seq = [aa+i for i in range(w*h)]

    expect = np.vstack([np.hstack(row)
                        for row in toolz.partition_all(w, seq)])

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

    aa = np.zeros((10, 2, 3, 3), dtype='int8')
    seq = [aa+i for i in range(w*h)]

    cc = _stack_2d_np(shape, *seq, axis=1)
    assert cc.shape == (10, 8, 9, 3)
    if verbose:
        print()
        print(cc[0, :, :, 0])


@pytest.mark.parametrize("span, chunks, summed, bspan, pspan", [
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
])
def test_chunk_range(span, chunks, summed, bspan, pspan):
    _bspan, _pspan = compute_chunk_range(span, chunks, summed)
    assert _bspan == bspan
    assert _pspan == pspan


@pytest.mark.parametrize("yx_roi", [
    np.s_[:, :],
    np.s_[:1, :1],
    np.s_[3:, 1:],
    np.s_[3:-3, 1:-5],
    np.s_[3:-3, -5:],
])
def test_crop_2d_dense(yx_roi):
    xx = da.random.uniform(0, 10, size=(16, 6),
                           chunks=(4, 3)).astype('uint8')

    yy = crop_2d_dense(xx, yx_roi)
    assert xx.dtype == yy.dtype
    assert yy.shape == xx[yx_roi].shape
    assert yy.shape == yy.chunksize

    assert (xx[yx_roi].compute() == yy.compute()).all()
