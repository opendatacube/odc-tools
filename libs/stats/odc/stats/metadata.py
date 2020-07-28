from typing import Dict, Any, Tuple
from copy import deepcopy
from datetime import datetime, timezone, timedelta
import click

from datacube.model import GridSpec
from odc import dscache
from odc.index import odc_uuid
from .model import OutputProduct, Task


def render_task_metadata(task: Task, ext: str = 'tiff') -> Dict[str, Any]:
    product = task.product
    geobox = task.geobox
    region_code = product.region_code(task.tile_index)
    properties = deepcopy(product.properties)

    properties['dtr:start_datetime'] = task.start_datetime.isoformat()
    properties['dtr:end_datetime'] = task.end_datetime.isoformat()
    properties['odc:region_code'] = region_code

    measurements = {band: {'path': path}
                    for band, path in task.paths().items()}

    return {
        '$schema': 'https://schemas.opendatacube.org/dataset',
        'id': str(task.uuid),
        'product': product.info,
        'location': task.metadata_path('absolute', ext='yaml'),

        'crs': str(geobox.crs),
        'grids': {'default': dict(shape=list(geobox.shape),
                                  transform=list(geobox.transform))},

        'measurements': measurements,
        'properties': properties,
        'lineage': {'inputs': [str(ds) for ds in task.input_dataset_ids]},
    }


def clear_pixel_count_product(gridspec: GridSpec) -> OutputProduct:
    name = 'ga_s2_clear_pixel_count'
    short_name = 'deafrica_s2_cpc'
    version = '0.0.0'

    info = {
        'name': name,
        'href': f'https://collections.digitalearth.africa/product/{name}'
    }

    algo_info = {
        'algorithm': 'odc-stats',
        'algorithm_version': '???',
        'deployment_id': '???'
    }

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
                         info=info,
                         short_name=short_name,
                         algo_info=algo_info,
                         location=location,
                         properties=properties,
                         measurements=measurements,
                         gridspec=gridspec)


# TODO: assumes annual only for now
def load_task(cache: dscache.DatasetCache,
              tile_index: Tuple[int, int],
              product: OutputProduct,
              year: int = 0,
              grid: str = '') -> Task:
    if grid == '':
        grid, *_ = cache.grids

    gridspec = cache.grids[grid]
    dss = list(ds for ds in cache.stream_grid_tile(tile_index, grid))
    input_dataset_ids = tuple(ds.id for ds in dss)
    geobox = gridspec.tile_geobox(tile_index)

    if year == 0:
        raise NotImplementedError("TODO: compute time period from datasets")

    start_datetime = datetime(year, 1, 1, tzinfo=timezone.utc)
    end_datetime = datetime(year+1, 1, 1, tzinfo=timezone.utc) - timedelta(microseconds=1)
    short_time = f'{year}--P1Y'

    return Task(product=product,
                tile_index=tile_index,
                geobox=geobox,
                start_datetime=start_datetime,
                end_datetime=end_datetime,
                short_time=short_time,
                input_dataset_ids=input_dataset_ids)


@click.command()
@click.argument('cache_path', type=str)
def main(cache_path):
    cache = dscache.open_rw(cache_path)
    grid = list(cache.grids)[0]
    gridspec = cache.grids[grid]
    year = 2020

    output_product = clear_pixel_count_product(gridspec)

    for tile_index, _ in cache.tiles(grid):
        task = load_task(cache, tile_index, output_product, year=year)

        d = render_task_metadata(task)
        cache._db.append_info_dict(f"tasks/{tile_index[0]}/{tile_index[1]}", d)


if __name__ == '__main__':
    main()
