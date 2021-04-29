"""
Misc numeric tooling
"""
from typing import Optional, Tuple
import numpy as np

from ._types import NumpyIndex1, NumpyIndex2, NumpyIndex


def half_up(n: int) -> int:
    """
    Divide by 2 and round up when input is odd.

    even -> n//2
    odd  -> n//2 + 1
    """
    return (n + 1) // 2


def np_slice_to_idx(idx: NumpyIndex1, n: int) -> Tuple[int, ...]:
    """
    Convert slice into a tuple of 0-based indexes
    """
    ii = np.arange(n, dtype=np.int32)[idx]
    if isinstance(ii, np.int32):
        return (int(ii),)
    return tuple(ii)


def roundup2(x: int) -> int:
    """
    @returns smallest integer Y that satisfies: (Y%2 == 0) and (Y >= x)
    """
    return (x + 1) & (~0x1)


def roundup16(x: int) -> int:
    """
    @returns smallest integer Y that satisfies: (Y%16 == 0) and (Y >= x)
    """
    return (x + 15) & (~0xF)


def roi_shrink2(idx: NumpyIndex, axis: int = 0) -> NumpyIndex:
    """
    Shrink 2d array slice

    :param idx: Slice into full sized image.
    :param axis: Index of the Y axis, assumed to be 0 if not supplied. For example for (B, Y, X) supply axis=1.
    """

    def maybe_half(x: Optional[int]) -> Optional[int]:
        if x is None:
            return None
        return half_up(x)

    def _shrink2(idx: NumpyIndex1) -> NumpyIndex1:
        if isinstance(idx, int):
            return half_up(idx)
        if isinstance(idx, slice):
            return slice(
                maybe_half(idx.start), maybe_half(idx.stop), maybe_half(idx.step)
            )

    return idx[:axis] + tuple(_shrink2(i) for i in idx[axis:axis+2]) + idx[axis+2:]


def shape_shrink2(shape: Tuple[int, ...], axis: int = 0) -> Tuple[int, ...]:
    """
    Given a shape compute half sized image shape

    :param shape: Input image shape, default order is (Y, X, [Band])
    :param axis: Index of the Y axis, assumed to be 0 if not supplied. For example for (B, Y, X) supply axis=1.
    """
    n1, n2 = map(half_up, shape[axis : axis + 2])
    return shape[:axis] + (n1, n2) + shape[axis + 2 :]


def work_dtype(dtype: np.dtype) -> np.dtype:
    """
    For integer types return {u}int32 for {u}int{8,16} and {u}int64 for others.
    For non-integer types returns input dtype.
    """
    if dtype.kind == "u":
        if dtype.itemsize < 4:
            return np.dtype("uint32")
        else:
            return np.dtype("uint64")
    elif dtype.kind == "i":
        if dtype.itemsize < 4:
            return np.dtype("int32")
        else:
            return np.dtype("int64")
    return dtype
