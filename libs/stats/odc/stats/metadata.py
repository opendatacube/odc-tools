from collections import OrderedDict
from uuid import UUID
from typing import Sequence, Tuple, Dict
import json

import yaml
import click

from odc import dscache
from odc.index import odc_uuid
from odc.dscache._dscache import mk_group_name

def represent_dictionary_order(self, dict_data):
    return self.represent_mapping('tag:yaml.org,2002:map', dict_data.items())

yaml.add_representer(OrderedDict, represent_dictionary_order)


def metadata_dict(product: Dict[str, str],
                  gridspec,
                  tile: Tuple[int, int],
                  time,
                  period: str,
                  algo_info,
                  input_product_tag: str,
                  input_dataset_ids: Sequence[UUID],
                  location: str,
                  properties: Dict,
                  measurements: Dict):

    geobox = gridspec.tile_geobox(tile)
    dataset_id = odc_uuid(sources=input_dataset_ids,
                          other_tags=dict(product=product, tile=tile, time=time, period=period),
                          **algo_info)

    result = OrderedDict()
    result['$schema'] = 'https://schemas.opendatacube.org/dataset'
    result['id'] = str(dataset_id)
    result['product'] = product
    result['crs'] = str(gridspec.crs)

    # roundtrip converts tuples to lists
    result['geometry'] = json.loads(json.dumps(geobox.extent.geom.__geo_interface__))

    result['grids'] = {'default': dict(shape=list(geobox.shape),
                                       transform=list(geobox.affine))}

    result['properties'] = properties

    result['location'] = location
    result['measurements'] = measurements

    result['lineage'] = {input_product_tag: [str(ds) for ds in input_dataset_ids]}

    return result


def band_filepath(product_name, product_version, tile, time, period, band_name):
    return f'{product_name}_v{product_version}_{tile[0]:-03d}_{tile[1]:-03d}_{time}--P{period}_{band_name}.tif'


def generate_clear_pixel_metadata(cache, grid, time, period):
    for tile, _ in cache.tiles(grid):
        gridspec = cache.grids[grid]

        product_name = 'ga_s2_clear_pixel_count'
        product_version = '0.0.0'

        product = OrderedDict()
        product['name'] = product_name
        product['href'] = f'https://collections.digitalearth.africa/product/{product_name}'

        group = mk_group_name(tile, grid)
        input_dataset_ids = [x.id for x in cache.stream_group(group)]

        algo_info = OrderedDict()
        algo_info['algorithm'] = 'odc-stats'
        algo_info['algorithm_version'] = '???'
        algo_info['deployment_id'] = '???'

        measurements = OrderedDict()
        for band_name in ['clear', 'total']:
            measurements[band_name] = {'path': band_filepath(product_name,
                                                             product_version,
                                                             tile,
                                                             time,
                                                             period,
                                                             band_name)}

        properties = OrderedDict()
        properties['datetime'] = time # ????
        properties['odc:dataset_version'] = '3.1.0'
        properties['odc:file_format'] = 'GeoTIFF'
        properties['odc:producer'] = 'ga.gov.au'
        properties['odc:product_family'] = 'statistics'
        properties['region_code'] = '{:-03d}_{:-03d}'.format(tile[0], tile[1])

        bucket_name = 'deafrica-stats-processing'
        location = f's3://{bucket_name}/{product_name}/v{product_version}/{tile[0]:-03d}/{tile[1]:-03d}/{time}--P{period}'

        yield metadata_dict(product=product,
                            gridspec=gridspec,
                            tile=tile,
                            input_product_tag='s2_l2a',
                            input_dataset_ids=input_dataset_ids,
                            time=time,
                            period=period,
                            algo_info=algo_info,
                            location=location,
                            properties=properties,
                            measurements=measurements)

def main(cache_path):
    cache = dscache.open_rw(cache_path)
    grid = list(cache.grids)[0]
    time = '2020'
    period = '1Y'

    for d in generate_clear_pixel_metadata(cache, grid, time, period):
        cache._db.append_info_dict(f"datasets/{d['id']}/", d)

if __name__ == '__main__':
    main('database')
