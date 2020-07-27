from uuid import UUID
from typing import Sequence, Tuple, Dict
from copy import deepcopy
from datetime import datetime, timezone, timedelta

import yaml
import click

from odc import dscache
from odc.index import odc_uuid
from odc.dscache._dscache import mk_group_name

from .model import OutputProduct, Task


def task_to_metadata_dict(task: Task) -> Dict:
    geobox = task.gridspec.tile_geobox(task.tile_index)
    region_code = "".join(tile_index_str(task.tile_index))
    output_product = task.output_product
    properties = deepcopy(output_product.properties)

    dataset_id = odc_uuid(sources=task.input_dataset_ids,
                          other_tags=dict(product=output_product.product_info,
                                          product_version=output_product.product_version,
                                          tile=task.tile_index,
                                          start_datetime=task.start_datetime,
                                          end_datetime=task.end_datetime),
                          **output_product.algo_info)

    properties['datetime'] = task.start_datetime.isoformat()
    properties['dtr:start_datetime'] = task.start_datetime.isoformat()
    properties['dtr:end_datetime'] = task.end_datetime.isoformat()
    properties['odc:processing_datetime'] = datetime.utcnow().replace(tzinfo=timezone.utc).isoformat()
    properties['odc:region_code'] = region_code

    location = '{}/{}/{}'.format(output_product.location,
                                 "/".join(tile_index_str(task.tile_index)),
                                 task.short_time)

    product_short_name = output_product.product_short_name
    measurements = {band_name: {'path': f'{product_short_name}_{region_code}_{task.short_time}_{band_name}.tif'}
                    for band_name in output_product.measurements}

    return {
        '$schema': 'https://schemas.opendatacube.org/dataset',
        'id': str(dataset_id),
        'product': task.output_product.product_info,
        'crs': str(task.gridspec.crs),

        'grids': {'default': dict(shape=list(geobox.shape),
                                  transform=list(geobox.affine))},

        'properties': properties,

        'location': location,
        'measurements': measurements,

        'lineage': {'inputs': [str(ds) for ds in task.input_dataset_ids]}
    }


def tile_index_str(tile_index):
    return [f'{dim}{index:+04d}'
            for dim, index in zip(['x', 'y'], tile_index)]


def clear_pixel_count_product() -> OutputProduct:
    product_name = 'ga_s2_clear_pixel_count'
    product_short_name = 'deafrica_s2_cpc'
    product_version = '0.0.0'

    product_info = {
        'name': product_name,
        'href': f'https://collections.digitalearth.africa/product/{product_name}'
    }

    algo_info = {
        'algorithm': 'odc-stats',
        'algorithm_version': '???',
        'deployment_id': '???'
    }

    bucket_name = 'deafrica-stats-processing'
    location = f's3://{bucket_name}/{product_name}/v{product_version}'
    measurements = ['clear', 'total']

    properties = {
        'odc:dataset_version': '3.1.0',
        'odc:file_format': 'GeoTIFF',
        'odc:producer': 'ga.gov.au',
        'odc:product_family': 'pixel_quality_statistics'
    }

    return OutputProduct(product_name=product_name,
                         product_version=product_version,
                         product_info=product_info,
                         product_short_name=product_short_name,
                         algo_info=algo_info,
                         location=location,
                         properties=properties,
                         measurements=measurements)


@click.command()
@click.argument('cache_path', type=str)
def main(cache_path):
    cache = dscache.open_rw(cache_path)
    grid = list(cache.grids)[0]
    gridspec = cache.grids[grid]

    start_datetime = datetime(2020, 1, 1, tzinfo=timezone.utc)
    end_datetime = datetime(2021, 1, 1, tzinfo=timezone.utc) - timedelta(microseconds=1)
    short_time = '2020--P1Y'

    output_product = clear_pixel_count_product()

    for tile_index, _ in cache.tiles(grid):
        input_dataset_ids = [x.id for x in cache.stream_group(mk_group_name(tile_index, grid))]

        task = Task(output_product=output_product,
                    gridspec=gridspec,
                    start_datetime=start_datetime,
                    end_datetime=end_datetime,
                    short_time=short_time,
                    tile_index=tile_index,
                    input_dataset_ids=input_dataset_ids)

        d = task_to_metadata_dict(task)
        region_code = "".join(tile_index_str(tile_index))
        cache._db.append_info_dict(f"tasks/{region_code}/", d)


if __name__ == '__main__':
    main()
