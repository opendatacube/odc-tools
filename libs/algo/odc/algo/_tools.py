"""
Various utilities
"""
from typing import Tuple, Union, Optional

ROI = Union[slice, Tuple[slice, ...]]


def slice_in_out(s: slice, n: int) -> Tuple[int, int]:
    def fill_if_none(x: Optional[int], val_if_none: int) -> int:
        return val_if_none if x is None else x

    start = fill_if_none(s.start, 0)
    stop = fill_if_none(s.stop, n)
    start, stop = [x if x >= 0 else n + x for x in (start, stop)]
    return (start, stop)


def roi_shape(
    roi: ROI, shape: Optional[Union[int, Tuple[int, ...]]] = None
) -> Tuple[int, ...]:
    if isinstance(shape, int):
        shape = (shape,)

    if isinstance(roi, slice):
        roi = (roi,)

    if shape is None:
        # Assume slices are normalised
        return tuple(s.stop - (s.start or 0) for s in roi)

    return tuple(
        _out - _in for _in, _out in (slice_in_out(s, n) for s, n in zip(roi, shape))
    )
