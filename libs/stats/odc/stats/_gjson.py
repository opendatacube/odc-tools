import math
from copy import deepcopy
import toolz
from typing import Tuple, Dict, Any
from datetime import timedelta

from datacube.model import GridSpec
from datacube.utils.geometry import polygon_from_transform, Geometry
from odc.index import solar_offset
from .model import TileIdx_xy, TileIdx_txy


def gs_bounds(gs: GridSpec, tiles: Tuple[Tuple[int, int], Tuple[int, int]]) -> Geometry:
    """
    Compute Polygon for a selection of tiles.

    :param gs: GridSpec
    :param tiles: (x_range, y_range)

    X,Y ranges are inclusive on the left and exclusive on the right, same as numpy slicing.
    """
    ((x0, x1), (y0, y1)) = tiles
    if gs.resolution[0] < 0:
        gb = gs.tile_geobox((x0, y1 - 1))
    else:
        gb = gs.tile_geobox((x0, y0))

    nx = (x1 - x0) * gb.shape[1]
    ny = (y1 - y0) * gb.shape[0]
    return polygon_from_transform(nx, ny, gb.affine, gb.crs)


def timedelta_to_hours(td: timedelta) -> float:
    return td.days * 24 + td.seconds / 3600


def compute_grid_info(
    cells: Dict[TileIdx_xy, Any], resolution: float = math.inf, title_width: int = 0
) -> Dict[TileIdx_xy, Any]:
    """
    Compute geojson feature for every cell in ``cells``.
    Where ``cells`` is produced by ``odc.index.bin_dataset_stream``
    """
    if title_width == 0:
        nmax = max([max(abs(ix), abs(iy)) for ix, iy in cells])
        # title_width is the number of digits in the index
        title_width = len(str(nmax))

    grid_info = {}

    for idx, cell in cells.items():
        geobox = cell.geobox
        utc_offset = timedelta_to_hours(solar_offset(geobox.extent))
        wrapdateline = utc_offset <= -11 or utc_offset >= 11

        geom = geobox.extent.to_crs(
            "epsg:4326", resolution=resolution, wrapdateline=wrapdateline
        )
        ix, iy = idx
        grid_info[idx] = {
            "type": "Feature",
            "geometry": geom.json,
            "properties": {
                "title": f"{ix:0{title_width}d},{iy:0{title_width}d}",
                "region_code": f"x{ix:0{title_width}d}y{iy:0{title_width}d}",
                "ix": ix,
                "iy": iy,
                "utc_offset": utc_offset,
                "total": len(cell.dss),
            },
        }

    return grid_info


def gjson_from_tasks(
    tasks: Dict[TileIdx_txy, Any], grid_info: Dict[TileIdx_xy, Any]
) -> Dict[str, Dict[str, Any]]:
    """
    Group tasks by time period and compute geosjon describing every tile covered by each time period.

    Returns time_period -> GeoJSON mapping

    Each feature in GeoJSON describes one tile and has following propreties

      .total      -- number of datasets
      .days       -- number of days with at least one observation
      .utc_offset -- utc_offset used to compute day boundaries
    """

    def _get(idx):
        xy_idx = idx[-2:]
        geo = deepcopy(grid_info[xy_idx])

        dss = tasks[idx]
        utc_offset = timedelta(hours=geo["properties"]["utc_offset"])

        ndays = len(set((ds.time + utc_offset).date() for ds in dss))
        geo["properties"]["total"] = len(dss)
        geo["properties"]["days"] = ndays

        return geo

    def process(idxs):
        return dict(type="FeatureCollection", features=[_get(idx) for idx in idxs])

    return {
        t: process(idxs)
        for t, idxs in toolz.groupby(toolz.first, sorted(tasks)).items()
    }
