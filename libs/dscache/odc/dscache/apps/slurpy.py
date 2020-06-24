import click
from threading import Thread
import queue
import datacube
from odc import dscache
from odc.dscache.tools import db_connect, raw_dataset_stream, mk_raw2ds
from odc.dscache.tools import dictionary_from_product_list
from odc.dscache.tools.tiling import parse_gridspec
from odc.dscache._dscache import doc2ds
from odc.index import bin_dataset_stream


EOS = object()


def qmap(proc, q, eos_marker=None):
    """ Converts queue to an iterator.

    For every `item` in the `q` that is not `eos_marker`, `yield proc(item)`
    """
    while True:
        item = q.get(block=True)
        if item is eos_marker:
            q.task_done()
            break
        else:
            yield proc(item)
            q.task_done()


@click.command('slurpy')
@click.option('--env', '-E', type=str, help='Datacube environment name')
@click.option('-z', 'complevel', type=int, default=6, help='Compression setting for zstandard 1-fast, 9+ good but slow')
@click.option('--grid', type=str,
              help="Grid spec or name 'crs;pixel_resolution;shape_in_pixels'",
              default=None)
@click.argument('output', type=str, nargs=1)
@click.argument('products', type=str, nargs=-1)
def cli(env, grid, output, products, complevel):

    if len(products) == 0:
        click.echo('Have to supply at least one product')
        raise click.Abort()

    dc = datacube.Datacube(env=env)
    all_prods = {p.name: p
                 for p in dc.index.products.get_all()}

    if len(products) == 1 and products[0].lower() in (':all:', '*'):
        click.echo('Will read all products')
        products = list(all_prods)

    for p in products:
        if p not in all_prods:
            click.echo('No such product found: %s' % p)
            raise click.Abort()

    click.echo('Getting dataset counts')
    counts = {p.name: count
              for p, count in dc.index.datasets.count_by_product(product=[p for p in products])}

    n_total = 0
    for p, c in counts.items():
        click.echo('..{}: {:8,d}'.format(p, c))
        n_total += c

    if n_total == 0:
        click.echo("No datasets found")
        raise click.Abort()

    click.echo('Training compression dictionary')
    zdict = dictionary_from_product_list(dc, products, samples_per_product=50)
    click.echo('..done')

    # TODO: check for overwrite
    cache = dscache.create_cache(output, zdict=zdict, complevel=complevel, truncate=True)

    raw2ds = mk_raw2ds(all_prods)

    def db_task(products, conn, q):
        for p in products:
            for ds in map(raw2ds, raw_dataset_stream(p, conn)):
                q.put(ds)
        q.put(EOS)

    conn = db_connect(cfg=env)
    q = queue.Queue(maxsize=10_000)
    db_thread = Thread(target=db_task, args=(products, conn, q))
    db_thread.start()

    dss = qmap(lambda ds: ds, q, eos_marker=EOS)
    dss = cache.tee(dss)

    cells = {}
    if grid is not None:
        gs = parse_gridspec(grid)
        # TODO for named gridspecs should we use the name as group_prefix?
        group_prefix = f"epsg{gs.crs.epsg:d}"
        cache.add_grid(gs, group_prefix)
        dss = bin_dataset_stream(gs, (doc2ds(doc, cache.products) for uuid, doc in dss), cells)

    label = 'Processing ({:8,d})'.format(n_total)
    with click.progressbar(dss, label=label, length=n_total) as dss:
        for ds in dss:
            pass

    if grid is not None:
        click.echo('Total bins: {:d}'.format(len(cells)))

        with click.progressbar(cells.values(), length=len(cells), label='Saving') as groups:
            for group in groups:
                cache.add_grid_tile(group_prefix, group.idx, group.dss)

    db_thread.join()
    cache.close()


if __name__ == '__main__':
    cli()
