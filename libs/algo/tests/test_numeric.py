import pytest
import numpy as np
from odc.algo._numeric import (
    half_up,
    np_slice_to_idx,
    roundup16,
    shape_shrink2,
    roi_shrink2,
)


def test_utils():
    assert half_up(4) == 2
    assert half_up(5) == 3
    for i in (1, 127, 24, 8889, 101010):
        assert half_up(i * 2) == i
        assert half_up(i * 2 + 1) == i + 1

    assert roundup16(16) == 16
    assert roundup16(17) == 32
    assert roundup16(31) == 32
    assert roundup16(32) == 32

    assert np_slice_to_idx(np.s_[:], 3) == (0, 1, 2)
    assert np_slice_to_idx(np.s_[:3], 10) == (0, 1, 2)
    assert np_slice_to_idx(np.s_[-1:], 10) == (9,)
    assert np_slice_to_idx(np.s_[0], 10) == (0,)
    assert np_slice_to_idx(np.s_[5], 10) == (5,)


@pytest.mark.parametrize(
    "shape,axis,expect",
    [
        ((4, 2), 0, (2, 1)),
        ((5, 2), 0, (3, 1)),
        ((6, 2), 0, (3, 1)),
        ((7, 9, 3), 0, (4, 5, 3)),
        ((7, 9, 3), 1, (7, 5, 2)),
        ((10, 16, 8, 3), 1, (10, 8, 4, 3)),
    ],
)
def test_shape_shrink2(shape, axis, expect):
    assert shape_shrink2(shape, axis=axis) == expect


@pytest.mark.parametrize(
    "roi,axis,expect",
    [
        (np.s_[10:20, 20:30, :], 0, np.s_[5:10, 10:15, :]),
        (np.s_[:,:], 0, np.s_[:, :]),
        (np.s_[10:20, :30, :], 0, np.s_[5:10, :15, :]),
        (np.s_[:, 10:20, 20:30, :], 1, np.s_[:, 5:10, 10:15, :]),
        (np.s_[:, 10:21, 20:31, :], 1, np.s_[:, 5:11, 10:16, :]),
    ],
)
def test_roi_shrink2(roi, axis, expect):
    assert roi_shrink2(roi, axis=axis) == expect
