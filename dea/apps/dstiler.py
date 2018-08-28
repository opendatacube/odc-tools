import click
import dscache
from .tiling import bin_dataset_stream
from datacube.model import GridSpec
import datacube.utils.geometry as geom


GS_ALBERS = GridSpec(crs=geom.CRS('EPSG:3577'),
                     tile_size=(100000.0, 100000.0),
                     resolution=(-25, 25))


@click.command('dstiler')
@click.argument('dbfile', type=str, nargs=1)
def cli(dbfile):
    cache = dscache.open_rw(dbfile)
    label = 'Processing {} ({:,d} datasets)'.format(dbfile, cache.count)

    gs = GS_ALBERS  # TODO: make configurable

    with click.progressbar(cache.get_all(), length=cache.count, label=label) as dss:
        bins = bin_dataset_stream(gs, dss)

    group_key_fmt = 'albers/{:+03d}{:+03d}'
    click.echo('Total bins: {:d}'.format(len(bins)))

    with click.progressbar(bins.values(), length=len(bins), label='Saving') as groups:
        for group in groups:
            k = group_key_fmt.format(*group.idx)
            cache.put_group(k, group.dss)


if __name__ == '__main__':
    cli()
