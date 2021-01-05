import toolz
from typing import Dict, Tuple, List, Any, Callable
from collections import namedtuple
from datetime import datetime
from .model import DateTimeRange

CompressedDataset = namedtuple("CompressedDataset", ["id", "time"])
Cell = Any


def _bin_generic(
    dss: List[CompressedDataset], bins: List[DateTimeRange]
) -> Dict[str, List[CompressedDataset]]:
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


def bin_generic(
    cells: Dict[Tuple[int, int], Cell], bins: List[DateTimeRange]
) -> Dict[Tuple[str, int, int], List[CompressedDataset]]:
    tasks: Dict[Tuple[str, int, int], List[CompressedDataset]] = {}
    for tidx, cell in cells.items():
        _bins = _bin_generic(cell.dss, bins)
        for t, dss in _bins.items():
            tasks[(t,) + tidx] = dss

    return tasks


def bin_seasonal(
    cells: Dict[Tuple[int, int], Cell], months: int, anchor: int
) -> Dict[Tuple[str, int, int], List[CompressedDataset]]:
    binner = season_binner(mk_season_rules(months, anchor))

    tasks = {}
    for tidx, cell in cells.items():
        # TODO: deal with UTC offsets for day boundary determination
        grouped = toolz.groupby(lambda ds: binner(ds.time), cell.dss)

        for temporal_k, dss in grouped.items():
            if temporal_k != "":
                tasks[(temporal_k,) + tidx] = dss

    return tasks


def bin_full_history(
    cells: Dict[Tuple[int, int], Cell], start: datetime, end: datetime
) -> Dict[Tuple[str, int, int], List[CompressedDataset]]:
    duration = end.year - start.year + 1
    temporal_key = (f"{start.year}--P{duration}Y",)
    return {temporal_key + k: cell.dss for k, cell in cells.items()}


def bin_annual(
    cells: Dict[Tuple[int, int], Cell]
) -> Dict[Tuple[str, int, int], List[CompressedDataset]]:
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


def mk_season_rules(months: int, anchor: int) -> Dict[int, str]:
    """
    Construct rules for a regular seasons
    :param months: Length of season in months can be one of (1,2,3,4,6,12)
    :param anchor: Start month of one of the seasons [1, 12]
    """
    assert months in (1, 2, 3, 4, 6, 12)
    assert 1 <= anchor <= 12

    rules: Dict[int, str] = {}
    for i in range(12 // months):
        start_month = anchor + i * months
        if start_month > 12:
            start_month -= 12

        for m in range(start_month, start_month + months):
            if m > 12:
                m = m - 12
            rules[m] = f"{start_month:02d}--P{months:d}M"

    return rules


def season_binner(rules: Dict[int, str]) -> Callable[[datetime], str]:
    """
    Construct mapping from datetime to a string in the form like 2010-06--P3M

    :param rules: Is a mapping from month (1-Jan, 2-Feb) to a string in the
                  form "{month:int}--P{N:int}M", where ``month`` is a starting
                  month of the season and ``N`` is a duration of the season in
                  months.
    """
    _rules: Dict[int, Tuple[str, int]] = {}

    for month in range(1, 12 + 1):
        season = rules.get(month, "")
        if season == "":
            _rules[month] = ("", 0)
        else:
            start_month = int(season.split("--")[0])
            _rules[month] = (season, 0 if start_month <= month else -1)

    def label(dt: datetime) -> str:
        season, yoffset = _rules[dt.month]
        if season == "":
            return ""
        y = dt.year + yoffset
        return f"{y}-{season}"

    return label


def dedup_s2_datasets(dss):
    """
    De-duplicate Sentinel 2 datasets. Datasets that share same timestamp and
    region code are considered to be duplicates.

    - Sort Datasets by ``(time, region code, label)``
    - Find groups of dataset that share common ``(time, region_code)``
    - Out of duplicate groups pick one with the most recent timestamp in the label (processing time)

    Above, ``label`` is something like this:
    ``S2B_MSIL2A_20190507T093039_N0212_R136_T32NPF_20190507T122231``

    The two timestamps are "capture time" and "processing time".

    :returns: Two list of Datasets, first one contains "freshest" datasets and
              has no duplicates, and the second one contains less fresh duplicates.
    """
    dss = sorted(
        dss,
        key=lambda ds: (
            ds.center_time,
            ds.metadata.region_code,
            ds.metadata_doc["label"],
        ),
    )
    out = []
    skipped = []

    for chunk in toolz.partitionby(
        lambda ds: (ds.center_time, ds.metadata.region_code), dss
    ):
        out.append(chunk[-1])
        skipped.extend(chunk[:-1])
    return out, skipped
