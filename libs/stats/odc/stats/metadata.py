import math
from copy import deepcopy
import toolz
from typing import Tuple, Dict, Any
from datetime import datetime, timedelta

from datacube.model import GridSpec
from datacube.utils.geometry import polygon_from_transform, Geometry
from odc import dscache
from odc.index import solar_offset
from .model import OutputProduct, Task, DateTimeRange


def gs_bounds(gs: GridSpec, tiles: Tuple[Tuple[int, int],
                                         Tuple[int, int]]) -> Geometry:
    """
    Compute Polygon for a selection of tiles.

    :param gs: GridSpec
    :param tiles: (x_range, y_range)

    X,Y ranges are inclusive on the left and exclusive on the right, same as numpy slicing.
    """
    ((x0, x1), (y0, y1)) = tiles
    if gs.resolution[0] < 0:
        gb = gs.tile_geobox((x0, y1-1))
    else:
        gb = gs.tile_geobox((x0, y0))

    nx = (x1 - x0)*gb.shape[1]
    ny = (y1 - y0)*gb.shape[0]
    return polygon_from_transform(nx, ny, gb.affine, gb.crs)


def clear_pixel_count_product(gridspec: GridSpec) -> OutputProduct:
    name = 'ga_s2_clear_pixel_count'
    short_name = 'deafrica_s2_cpc'
    version = '0.0.0'

    bucket_name = 'deafrica-stats-processing'
    location = f's3://{bucket_name}/{name}/v{version}'
    measurements = ('clear', 'total')

    properties = {
        'odc:file_format': 'GeoTIFF',
        'odc:producer': 'ga.gov.au',
        'odc:product_family': 'pixel_quality_statistics'
    }

    return OutputProduct(name=name,
                         version=version,
                         short_name=short_name,
                         location=location,
                         properties=properties,
                         measurements=measurements,
                         gridspec=gridspec,
                         href=f'https://collections.digitalearth.africa/product/{name}',
                         freq='1Y')


# TODO: assumes annual only for now
def load_task(cache: dscache.DatasetCache,
              tile_index: Tuple[int, int],
              product: OutputProduct,
              year: int = 0,
              grid: str = '') -> Task:
    if grid == '':
        grid, *_ = cache.grids

    gridspec = cache.grids[grid]
    dss = tuple(ds for ds in cache.stream_grid_tile(tile_index, grid))
    geobox = gridspec.tile_geobox(tile_index)

    if year == 0:
        raise NotImplementedError("TODO: compute time period from datasets")

    time_range = DateTimeRange(start=datetime(year=year, month=1, day=1),
                               freq=product.freq)

    return Task(product=product,
                tile_index=tile_index,
                geobox=geobox,
                time_range=time_range,
                datasets=dss)


def timedelta_to_hours(td: timedelta) -> float:
    return td.days*24 + td.seconds/3600


def compute_grid_info(cells: Dict[Tuple[int, int], Any],
                      resolution: float = math.inf,
                      title_width: int = 5) -> Dict[Tuple[int, int], Any]:
    """
    Compute geojson feature for every cell in ``cells``.
    Where ``cells`` is produced by ``odc.index.bin_dataset_stream``
    """
    grid_info = {}

    for idx, cell in cells.items():
        geobox = cell.geobox
        utc_offset = timedelta_to_hours(solar_offset(geobox.extent))
        wrapdateline = (utc_offset <= -11 or utc_offset >= 11)

        geom = geobox.extent.to_crs('epsg:4326',
                                    resolution=resolution,
                                    wrapdateline=wrapdateline)
        ix, iy = idx
        grid_info[idx] = {
            'type': 'Feature',
            'geometry': geom.json,
            'properties': {
                'title': f'{ix:+0{title_width}d},{iy:+0{title_width}d}',
                'utc_offset': utc_offset,
                'total': len(cell.dss),
            },
        }

    return grid_info


def gjson_from_tasks(tasks: Dict[Tuple[str, int, int], Any],
                     grid_info: Dict[Tuple[int, int], Any]) -> Dict[str, Dict[str, Any]]:
    """
    Group tasks by time period and computer geosjon describing every tile covered by each time period.

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
        utc_offset = timedelta(hours=geo['properties']['utc_offset'])

        ndays = len(set((ds.time+utc_offset).date()
                        for ds in dss))
        geo['properties']['total'] = len(dss)
        geo['properties']['days'] = ndays

        return geo

    def process(idxs):
        return dict(type='FeatureCollection',
                    features=[_get(idx) for idx in idxs])

    return {t: process(idxs)
            for t, idxs in toolz.groupby(toolz.first,
                                         sorted(tasks)).items()}
