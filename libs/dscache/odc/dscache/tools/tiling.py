from typing import Tuple, Optional, Dict
from math import pi
from types import SimpleNamespace
import toolz
from datacube.utils.geometry import CRS
from datacube.model import GridSpec, Dataset
from odc.io.text import split_and_check, parse_range_int


epsg3577 = CRS("epsg:3577")
epsg6933 = CRS("epsg:6933")

# Origin was chosen such that there are no negative indexed tiles for any valid
# point within a given CRS's valid region, and also making sure that x=0,y=0
# lines fall on tile edges.
#
GRIDS = {
    "albers_au_25": GridSpec(
        crs=epsg3577, tile_size=(100_000.0, 100_000.0), resolution=(-25, 25)
    ),
    "au": GridSpec(
        crs=epsg3577,
        tile_size=(96_000.0, 96_000.0),
        resolution=(-96_000, 96_000),
        origin=(-5088000, -2208000),
    ),
    **{
        f"au_{n}": GridSpec(
            crs=epsg3577,
            tile_size=(96_000.0, 96_000.0),
            resolution=(-n, n),
            origin=(-5088000, -2208000),
        )
        for n in (10, 20, 30, 60)
    },
    "global": GridSpec(
        crs=epsg6933,
        tile_size=(96_000.0, 96_000.0),
        resolution=(-96_000, 96_000),
        origin=(-7392000, -17376000),
    ),
    **{
        f"global_{n}": GridSpec(
            crs=epsg6933,
            tile_size=(96_000.0, 96_000.0),
            resolution=(-n, n),
            origin=(-7392000, -17376000),
        )
        for n in (10, 20, 30, 60)
    },
}


# Inject aliases for Africa
GRIDS["africa"] = GRIDS["global"]
for r in (10, 20, 30, 60):
    GRIDS[f"africa_{r}"] = GRIDS[f"global_{r}"]


def web_gs(zoom: int, tile_size: int = 256) -> GridSpec:
    """
    Construct grid spec compatible with TerriaJS requests at a given level.

    Tile indexes should be the same as google maps, except that Y component is negative,
    this is a limitation of GridSpec class, you can not have tile index direction be
    different from axis direction, but this is what google indexing is using.

    http://www.maptiler.org/google-maps-coordinates-tile-bounds-projection/
    """
    R = 6378137

    origin = pi * R
    res0 = 2 * pi * R / tile_size
    res = res0 * (2 ** (-zoom))
    tsz = 2 * pi * R * (2 ** (-zoom))  # res*tile_size

    return GridSpec(
        crs=CRS("epsg:3857"),
        tile_size=(tsz, tsz),
        resolution=(-res, res),  # Y,X
        origin=(origin - tsz, -origin),
    )  # Y,X


def extract_native_albers_tile(
    ds: Dataset, tile_size: float = 100_000.0
) -> Tuple[int, int]:
    ll = toolz.get_in(
        "grid_spatial.projection.geo_ref_points.ll".split("."), ds.metadata_doc
    )
    return (int(ll["x"] / tile_size), int(ll["y"] / tile_size))


def extract_ls_path_row(ds: Dataset) -> Optional[Tuple[int, int]]:
    full_id = ds.metadata_doc.get("tile_id")

    if full_id is None:
        full_id = toolz.get_in(["usgs", "scene_id"], ds.metadata_doc)

    if full_id is None:
        return None
    return (int(full_id[3:6]), int(full_id[6:9]))


def bin_by_native_tile(dss, cells, persist=None, native_tile_id=None):
    """Group datasets by native tiling, like path/row for Landsat.

    :param dss: Sequence of datasets (can be lazy)
    :param cells: Dictionary to populate with tiles

    :param persist: Dataset -> SomeThing mapping, defaults to keeping dataset
    id only

    :param native_tile_id: Dataset -> Key, defaults to extracting `path,row`
    tuple from metadata's `tile_id` field, but could be anything, only
    constraint is that Key value can be used as index to python dict.
    """

    def default_persist(ds):
        return ds.id

    def register(tile, val):
        cell = cells.get(tile)
        if cell is None:
            cells[tile] = SimpleNamespace(idx=tile, dss=[val])
        else:
            cell.dss.append(val)

    native_tile_id = native_tile_id or extract_ls_path_row
    persist = persist or default_persist

    for ds in dss:
        ds_val = persist(ds)
        tile = native_tile_id(ds)
        if tile is None:
            raise ValueError("Missing tile id")
        else:
            register(tile, ds_val)
        yield ds


def _parse_gridspec_string(s: str) -> GridSpec:
    """
    "epsg:6936;10;9600"
    "epsg:6936;-10x10;9600x9600"
    """

    crs, res, shape = split_and_check(s, ";", 3)
    try:
        if "x" in res:
            res = tuple(float(v) for v in split_and_check(res, "x", 2))
        else:
            res = float(res)
            res = (-res, res)

        if "x" in shape:
            shape = parse_range_int(shape, separator="x")
        else:
            shape = int(shape)
            shape = (shape, shape)
    except ValueError:
        raise ValueError(f"Failed to parse gridspec: {s}")

    tsz = tuple(abs(n * res) for n, res in zip(res, shape))

    return GridSpec(crs=CRS(crs), tile_size=tsz, resolution=res, origin=(0, 0))


def _norm_gridspec_name(s: str) -> str:
    return s.replace("-", "_")


def parse_gridspec(s: str, grids: Optional[Dict[str, GridSpec]] = None) -> GridSpec:
    """
    "africa_10"
    "epsg:6936;10;9600"
    "epsg:6936;-10x10;9600x9600"
    """
    if grids is None:
        grids = GRIDS

    named_gs = grids.get(_norm_gridspec_name(s))
    if named_gs is not None:
        return named_gs

    return _parse_gridspec_string(s)


def parse_gridspec_with_name(
    s: str, grids: Optional[Dict[str, GridSpec]] = None
) -> Tuple[str, GridSpec]:
    if grids is None:
        grids = GRIDS

    named_gs = grids.get(_norm_gridspec_name(s))
    if named_gs is not None:
        return (s, named_gs)

    gs = _parse_gridspec_string(s)
    s = s.replace(";", "_")
    return (s, gs)


def gridspec_from_crs(
    crs: CRS,
    tile_size: Tuple[float, float] = (96_000, 96_000),
    pad_yx: Tuple[int, int] = (0, 0),
    resolution: Optional[Tuple[float, float]] = None,
):
    """
    Compute GridSpec such that there are no negative tiles overlapping with the
    valid region of the target CRS.

    :param crs: Coordinate System used to define the grid
    :param tile_size: (Y, X) size of each tile, in CRS units
    :param pad_yx: (Y, X) safety margin in number of tiles
    :param resolution: (Y, X) size of each data point in the grid, in CRS units. Y will
                       usually be negative.
    """
    from math import floor

    if resolution is None:
        resolution = (-tile_size[0], tile_size[1])

    x0, y0, x1, y1 = crs.valid_region.to_crs(crs).boundingbox
    # index of the tile containing bottom left corner
    iy, ix = (int(floor(v / tsz)) for v, tsz in zip((y0, x0), tile_size))

    y0_, x0_ = [
        float((idx - pad) * tsz) for (idx, pad, tsz) in zip((iy, ix), pad_yx, tile_size)
    ]

    return GridSpec(crs, tile_size, resolution=resolution, origin=(y0_, x0_))
