""" Index datasets from tar arachive
"""

import os
import sys
from copy import deepcopy

import click
import datacube
from datacube.utils.changes import allow_any
from odc.index import from_yaml_doc_stream, odc_uuid
from odc.io.tar import tar_doc_stream, tar_mode
from odc.io.timer import RateEstimator
from pyproj import CRS, Transformer


KNOWN_CONSTELLATIONS = [
    'sentinel-2'
]


def _stac_product_lookup(item):
    product_label = item['id']
    product_name = item['properties']['platform']
    region_code = None

    constellation = item['properties']['constellation']

    if constellation in KNOWN_CONSTELLATIONS:
        if constellation == 'sentinel-2':
            product_label = item['properties']["sentinel:product_id"]
            product_split = product_label.split("_")
            product_name = f"{product_split[0]}_{product_split[1]}"
            region_code = "{}{}{}".format(
                str(item['properties']["proj:epsg"])[-2:],
                item['properties']["sentinel:latitude_band"],
                item['properties']["sentinel:grid_square"]
            )

    return product_label, product_name, region_code


def _get_stac_bands(item, default_grid='g10.0m'):
    bands = {}

    grids = {}

    assets = item['assets']

    for asset_name, asset in assets.items():
        # Ignore items that are not actual COGs
        if asset['type'] not in ["image/tiff; application=geotiff; profile=cloud-optimized"]:
            continue

        transform = asset['proj:transform']
        grid = "g{}m".format(transform[0])

        if grid not in grids:
            grids[grid] = {
                'shape': asset['proj:shape'],
                'transform': asset['proj:transform']
            }

        band_info = {
            'path': os.path.split((asset['href']))[1]
        }

        if grid != default_grid:
            band_info['grid'] = grid

        bands[asset_name] = band_info

    grids['default'] = grids[default_grid]
    del grids[default_grid]

    return bands, grids


def geographic_to_projected(geometry, target_srs):
    wgs = CRS.from_epsg(4326)
    projection = CRS.from_epsg(target_srs)

    transformer = Transformer.from_crs(wgs, projection)

    new_geometry = deepcopy(geometry)
    new_geometry['coordinates'][0] = [
        transformer.transform(p[0], p[1]) for p in new_geometry['coordinates'][0]]

    return new_geometry


def stac_transform(input_stac):
    """takes in a raw STAC 1.0 dictionary and returns an ODC dictionary
    """
    print("Starting STAC transform")

    # Dodgy lookup
    product_label, product_name, region_code = _stac_product_lookup(input_stac)

    # Make a proper deterministic UUID
    deterministic_uuid = str(odc_uuid("sentinel2_stac_process", "1.0.0", [product_label]))

    # Get grids and bands
    bands, grids = _get_stac_bands(input_stac)

    stac_odc = {
        '$schema': 'https://schemas.opendatacube.org/dataset',
        'id': deterministic_uuid,
        'crs': "epsg:{}".format(input_stac['properties']['proj:epsg']),
        'geometry': geographic_to_projected(
            input_stac['geometry'],
            input_stac['properties']['proj:epsg']
        ),
        'grids': grids,
        'product': {
            'name': product_name  # This is not right
        },
        'label': product_label,
        'properties': {
            'datetime': input_stac['properties']['datetime'].replace("000+00:00", "Z"),
            'odc:processing_datetime': input_stac['properties']['datetime']
                                       .replace("000+00:00", "Z"),
            'eo:cloud_cover': input_stac['properties']['eo:cloud_cover'],
            'eo:gsd': input_stac['properties']['gsd'],
            'eo:instrument': input_stac['properties']['instruments'][0],
            'eo:platform': input_stac['properties']['platform'],
            'odc:file_format': 'GeoTIFF'
        },
        'measurements': bands,
        'lineage': {}
    }

    if region_code:
        stac_odc['properties']['odc:region_code']: region_code

    # import json
    # with open(f'/tmp/{input_stac["id"]}.json', 'w') as outfile:
    #     json.dump(stac_odc, outfile, indent=4)

    return stac_odc


def from_tar_file(tarfname, index, mk_uri, mode, doc_transform=None, **kwargs):
    """ returns a sequence of tuples where each tuple is either

        (ds, None) or (None, error_message)
    """
    def untar(tarfname, mk_uri):
        for doc_name, doc in tar_doc_stream(tarfname, mode=mode):
            yield mk_uri(doc_name), doc

    return from_yaml_doc_stream(untar(tarfname, mk_uri), index, transform=doc_transform, **kwargs)


@click.command('index_from_tar')
@click.option('--env', '-E', type=str, help='Datacube environment name')
@click.option('--product', '-p', 'product_names',
              help=('Only match against products specified with this option, '
                    'you can supply several by repeating this option with a new product name'),
              multiple=True)
@click.option('--exclude-product', '-x', 'exclude_product_names',
              help=('Attempt to match to all products in the DB except for products '
                    'specified with this option, '
                    'you can supply several by repeating this option with a new product name'),
              multiple=True)
@click.option('--auto-add-lineage/--no-auto-add-lineage', is_flag=True, default=True,
              help=('Default behaviour is to automatically add lineage datasets if they are missing from the database, '
                    'but this can be disabled if lineage is expected to be present in the DB, '
                    'in this case add will abort when encountering missing lineage dataset'))
@click.option('--verify-lineage/--no-verify-lineage', is_flag=True, default=True,
              help=('Lineage referenced in the metadata document should be the same as in DB, '
                    'default behaviour is to skip those top-level datasets that have lineage data '
                    'different from the version in the DB. This option allows omitting verification step.'))
@click.option('--ignore-lineage',
              help="Pretend that there is no lineage data in the datasets being indexed",
              is_flag=True, default=False)
@click.option('--update',
              help="Update datasets rather than add",
              is_flag=True, default=False)
@click.option('--gzip', is_flag=True, help='Input is compressed with gzip (needed when reading from stdin)')
@click.option('--xz', is_flag=True, help='Input is compressed with xz (needed when reading from stdin)')
@click.option('--protocol', type=str, default='s3', show_default=True,
              help='Override the protocol for working with data in other environments, i.e gs')
@click.option('--stac', is_flag=True, default=False, show_default=True,
              help='Convert STAC 1.0 metadata to ODC EO3 metadata')
@click.argument('input_fname', type=str, nargs=-1)
def cli(input_fname,
        env,
        product_names,
        exclude_product_names,
        auto_add_lineage,
        verify_lineage,
        ignore_lineage,
        update,
        gzip,
        xz,
        protocol,
        stac):

    # Ensure :// is present in prefix
    prefix = protocol.rstrip('://') + '://'
    if prefix.startswith('file'):
        prefix = prefix + '/'

    if ignore_lineage:
        auto_add_lineage = False

    ds_resolve_args = dict(products=product_names,
                           exclude_products=exclude_product_names,
                           fail_on_missing_lineage=not auto_add_lineage,
                           verify_lineage=verify_lineage,
                           skip_lineage=ignore_lineage)

    allowed_changes = {(): allow_any}

    transform = None
    if stac:
        print("Doing stac transform")
        transform = stac_transform

    def mk_uri(name):
        return prefix + name

    def report_error(msg):
        print(msg, file=sys.stderr)

    def process_file(filename, index, fps, mode=None, n_failed=0, doc_transform=None):
        for ds, err in from_tar_file(filename, index, mk_uri, mode=mode, doc_transform=doc_transform, **ds_resolve_args):
            if ds is not None:
                try:
                    if update:
                        index.datasets.update(ds, allowed_changes)
                    else:
                        index.datasets.add(ds, with_lineage=auto_add_lineage)

                except Exception as e:
                    n_failed += 1
                    report_error(str(e))
            else:
                n_failed += 1
                report_error(err)

            fps()

            if fps.every(10):
                print('.', end='', flush=True)

            if fps.every(100):
                print(' {} F:{:d}'.format(str(fps), n_failed))

        return n_failed

    dc = datacube.Datacube(env=env)

    if len(input_fname) == 0:
        input_fname = ('-',)

    n_failed = 0
    fps = RateEstimator()
    mode = None

    for filename in input_fname:
        print(f"indexing {filename}")
        if filename == '-':
            if sys.stdin.isatty():
                report_error("Requesting to read from stdin but not redirecting input?")
                sys.exit(1)
            filename = sys.stdin.buffer
            mode = tar_mode(is_pipe=True, gzip=gzip, xz=xz)

        n_failed = process_file(filename, dc.index, fps, mode=mode, n_failed=n_failed, doc_transform=transform)

    if n_failed > 0:
        report_error("**WARNING** there were failures: {}".format(n_failed))


if __name__ == '__main__':
    cli()
