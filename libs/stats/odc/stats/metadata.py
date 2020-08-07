from typing import Tuple
from datetime import datetime

from datacube.model import GridSpec
from datacube.utils.geometry import polygon_from_transform, Geometry
from odc import dscache
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
