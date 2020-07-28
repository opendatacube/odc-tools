from typing import Tuple
from datetime import datetime
import click

from datacube.model import GridSpec
from odc import dscache
from .model import OutputProduct, Task, DateTimeRange


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


@click.command()
@click.argument('cache_path', type=str)
def main(cache_path):
    cache = dscache.open_rw(cache_path)
    grid = list(cache.grids)[0]
    gridspec = cache.grids[grid]
    year = 2020  # TODO: read from cache stats info section

    output_product = clear_pixel_count_product(gridspec)

    for tile_index, _ in cache.tiles(grid):
        task = load_task(cache, tile_index, output_product, year=year)

        d = task.render_metadata()
        cache._db.append_info_dict(f"tasks/{tile_index[0]}/{tile_index[1]}", d)


if __name__ == '__main__':
    main()
