import toolz
from typing import Dict, Tuple, List, Any, Callable, Optional
from collections import namedtuple
from datetime import datetime
from .model import DateTimeRange
from odc.index import odc_uuid
from datacube.storage import measurement_paths
from datacube.model import Dataset, DatasetType
from datacube.index.eo3 import prep_eo3


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
        utc_offset = cell.utc_offset
        grouped = toolz.groupby(lambda ds: binner(ds.time + utc_offset), cell.dss)

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
        utc_offset = cell.utc_offset
        grouped = toolz.groupby(lambda ds: (ds.time + utc_offset).year, cell.dss)

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
            if months == 12:
                rules[m] = f"{start_month:02d}--P1Y"
            else:
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


def fuse_products(type_1: DatasetType, type_2: DatasetType) -> DatasetType:
    """
    Fuses two products. This function requires access to a Datacube to access the metadata type.

    Fusing two products requires that:
      - both metadata types are eo3
      - there are no conflicting band names
      - the file formats are identical
    """

    def_1, def_2 = type_1.definition, type_2.definition
    fused_def = dict()

    assert def_1["metadata_type"] == def_2["metadata_type"]
    assert def_1["metadata_type"] == "eo3"

    measurements_1 = set(m["name"] for m in def_1["measurements"])
    measurements_2 = set(m["name"] for m in def_2["measurements"])
    assert len(measurements_1.intersection(measurements_2)) == 0

    file_format = def_1["metadata"]["properties"]["odc:file_format"]
    assert file_format == def_2["metadata"]["properties"]["odc:file_format"]

    name = f"fused__{def_1['name']}__{def_2['name']}"

    fused_def["name"] = name
    fused_def["metadata"] = {
        "product": {"name": name},
        "properties": {"odc:file_format": file_format},
    }
    fused_def[
        "description"
    ] = f"Fused products: {def_1['description']}, {def_2['description']}"
    fused_def["measurements"] = def_1["measurements"] + def_2["measurements"]
    fused_def["metadata_type"] = def_1["metadata_type"]

    return DatasetType(type_1.metadata_type, fused_def)


def fuse_ds(
    ds_1: Dataset, ds_2: Dataset, product: Optional[DatasetType] = None
) -> Dataset:
    """
    This function fuses two datasets. It requires that:
      - the products are fusable
      - grids with the same name are identical
      - labels are in the format 'product_suffix' with identical suffixes
      - CRSs' are identical
      - datetimes are identical
      - $schemas are identical
    """

    doc_1, doc_2 = ds_1.metadata_doc, ds_2.metadata_doc

    if product is None:
        product = fuse_products(ds_1.type, ds_2.type)

    fused_doc = dict()

    fused_doc["id"] = str(
        odc_uuid(product.name, "0.0.0", sources=[doc_1["id"], doc_2["id"]])
    )
    fused_doc["lineage"] = {"source_datasets": [doc_1["id"], doc_2["id"]]}

    # check that all grids with the same name are identical
    common_grids = set(doc_1["grids"].keys()).intersection(doc_2["grids"].keys())
    assert all(doc_1["grids"][g] == doc_2["grids"][g] for g in common_grids)

    # TODO: handle the case that grids have conflicts in a seperate function
    fused_doc["grids"] = {**doc_1["grids"], **doc_2["grids"]}

    label_suffix = doc_1["label"].replace(doc_1["product"]["name"], "")
    assert label_suffix == doc_2["label"].replace(doc_2["product"]["name"], "")
    fused_doc["label"] = f"{product.name}{label_suffix}"

    equal_keys = ["$schema", "crs"]
    for key in equal_keys:
        assert doc_1[key] == doc_2[key]
        fused_doc[key] = doc_1[key]

    fused_doc["properties"] = dict()
    assert (
        doc_1["properties"]["datetime"] == doc_2["properties"]["datetime"]
    )  # datetime is the only manditory property

    # copy over all identical properties
    for key, val in doc_1["properties"].items():
        if val == doc_2["properties"].get(key, None):
            fused_doc["properties"][key] = val

    fused_doc["measurements"] = {**doc_1["measurements"], **doc_2["measurements"]}
    for key, path in {**measurement_paths(ds_1), **measurement_paths(ds_2)}.items():
        fused_doc["measurements"][key]["path"] = path

    fused_ds = Dataset(product, prep_eo3(fused_doc), uris=[""])
    fused_doc["properties"]["fused"] = "True"
    return fused_ds
