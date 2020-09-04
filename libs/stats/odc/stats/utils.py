import toolz
from typing import Dict, Tuple, List, Any
from collections import namedtuple
from datetime import datetime

from .model import DateTimeRange

CompressedDataset = namedtuple("CompressedDataset", ['id', 'time'])
Cell = Any


def find_seasonal_bin(dt: datetime, months: int, anchor: int) -> DateTimeRange:
    dtr = DateTimeRange(f"{dt.year}-{anchor:02d}--P{months}M")
    if dt in dtr:
        return dtr
    step = 1 if dt > dtr else -1
    while dt not in dtr:
        dtr = dtr + step
    return dtr


def _bin_generic(dss: List[CompressedDataset],
                 bins: List[DateTimeRange]) -> Dict[str, List[CompressedDataset]]:
    """
    Dumb O(NM) implementation, N number of dataset, M number of bins.

    For every bin find all datasets that fall in there, and if not empty keep that bin.
    """
    out: Dict[str, List[CompressedDataset]] = {}
    for b in bins:
        _dss = [ds for ds in dss if ds.time in b]
        if len(_dss) > 0:
            out[b.short] = _dss

    return out


def bin_generic(cells: Dict[Tuple[int, int], Cell],
                bins: List[DateTimeRange]) -> Dict[Tuple[str, int, int], List[CompressedDataset]]:
    tasks: Dict[Tuple[str, int, int], List[CompressedDataset]] = {}
    for tidx, cell in cells.items():
        _bins = _bin_generic(cell.dss, bins)
        for t, dss in _bins.items():
            tasks[(t,) + tidx] = dss

    return tasks


def bin_seasonal(cells: Dict[Tuple[int, int], Cell],
                 start: datetime,
                 end: datetime,
                 months: int,
                 anchor: int) -> Dict[Tuple[str, int, int], List[CompressedDataset]]:
    assert months in (1, 2, 3, 4, 6)
    assert 1 <= anchor <= 12
    assert end >= start

    b = find_seasonal_bin(start, months, anchor)
    bins = []
    while b < end:
        bins.append(b)
        b = b + 1

    if end in b:
        bins.append(b)

    return bin_generic(cells, bins)


def bin_full_history(cells: Dict[Tuple[int, int], Cell],
                     start: datetime,
                     end: datetime) -> Dict[Tuple[str, int, int], List[CompressedDataset]]:
    duration = end.year - start.year + 1
    temporal_key = (f"{start.year}--P{duration}Y",)
    return {temporal_key + k: cell.dss
            for k, cell in cells.items()}


def bin_annual(cells: Dict[Tuple[int, int], Cell]) -> Dict[Tuple[str, int, int], List[CompressedDataset]]:
    """
    Annual binning
    :param cells: (x,y) -> Cell(dss: List[CompressedDataset], geobox: GeoBox, idx: Tuple[int, int])
    """
    tasks = {}
    for tidx, cell in cells.items():
        # TODO: deal with UTC offsets for day boundary determination
        grouped = toolz.groupby(lambda ds: ds.time.year, cell.dss)

        for year, dss in grouped.items():
            temporal_k = (f"{year}--P1Y",)
            tasks[temporal_k + tidx] = dss

    return tasks
