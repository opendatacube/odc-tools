import click
from odc import dscache
from odc.dscache.tools.tiling import bin_by_native_tile, web_gs, extract_native_albers_tile
from datacube.model import GridSpec
from odc.index import bin_dataset_stream

GS_ALBERS = GridSpec(crs='EPSG:3577',
                     tile_size=(100000.0, 100000.0),
                     resolution=(-25, 25))


@click.command('dstiler')
@click.option('--native', is_flag=True, help='Use Landsat Path/Row as grouping')
@click.option('--native-albers', is_flag=True, help='When datasets are in Albers grid already')
@click.option('--web', type=int, help='Use web map tiling regime at supplied zoom level')
@click.option('--crs', type=str, help="Custom gridspec: CRS code")
@click.option('--resolution', type=str, help="Custom gridspec: resolution in CRS units per pixel in y,x order")
@click.option('--shape', type=str, help="Custom gridspec: shape of tile in pixel in y,x order")
@click.option('--bin-format', type=str, help="Custom gridspec: format of bin group key")
@click.argument('dbfile', type=str, nargs=1)
def cli(native, native_albers, web, crs, resolution, shape, bin_format, dbfile):
    """Add spatial grouping to file db.

    Default grid is Australian Albers (EPSG:3577) with 100k by 100k tiles. But
    you can also group by Landsat path/row (--native), or Google's map tiling
    regime (--web zoom_level)

    """
    cache = dscache.open_rw(dbfile)
    label = 'Processing {} ({:,d} datasets)'.format(dbfile, cache.count)

    if native:
        group_key_fmt = 'native/{:03d}_{:03d}'
        binner = bin_by_native_tile
    elif native_albers:
        group_key_fmt = 'albers/{:03d}_{:03d}'
        binner = lambda dss: bin_by_native_tile(dss, native_tile_id=extract_native_albers_tile)
    elif web is not None:
        gs = web_gs(web)
        group_key_fmt = 'web_' + str(web) + '/{:03d}_{:03d}'
        binner = lambda dss: bin_dataset_stream(gs, dss)
    elif crs is not None or resolution is not None or shape is not None or bin_format is not None:
        settings = dict(crs=crs, resolution=resolution, shape=shape, bin_format=bin_format)
        missing = [key for key, value in settings.items() if value is None]
        if missing:
            raise ValueError('Missing options in custom grid spec: {}'.format(' '.join(missing)))
        group_key_fmt = bin_format
        resolution = [float(comp) for comp in resolution.split(',')]
        shape = [float(comp) for comp in shape.split(',')]
        tile_size = [abs(res * shp) for res, shp in zip(resolution, shape)]
        gs = GridSpec(crs=crs, tile_size=tile_size, resolution=resolution)
        binner = lambda dss: bin_dataset_stream(gs, dss)
    else:
        group_key_fmt = 'albers/{:03d}_{:03d}'
        binner = lambda dss: bin_dataset_stream(GS_ALBERS, dss)

    with click.progressbar(cache.get_all(), length=cache.count, label=label) as dss:
        bins = binner(dss)

    click.echo('Total bins: {:d}'.format(len(bins)))

    with click.progressbar(bins.values(), length=len(bins), label='Saving') as groups:
        for group in groups:
            k = group_key_fmt.format(*group.idx)
            cache.put_group(k, group.dss)


if __name__ == '__main__':
    cli()
