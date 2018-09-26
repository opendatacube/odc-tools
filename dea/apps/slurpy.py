import click
import datacube
import dscache
from dscache.tools import db_connect, raw_dataset_stream, mk_raw2ds
from dscache.tools import dictionary_from_product_list


@click.command('slurpy')
@click.option('--env', type=str, help='Datacube environment name')
@click.argument('output', type=str, nargs=1)
@click.argument('products', type=str, nargs=-1)
def cli(env, output, products):

    if len(products) == 0:
        click.echo('Have to supply at least one product')
        raise click.Abort()

    dc = datacube.Datacube(env=env)
    all_prods = {p.name: p
                 for p in dc.index.products.get_all()}

    for p in products:
        if p not in all_prods:
            click.echo('No such product found: %s' % p)
            raise click.Abort()

    raw2ds = mk_raw2ds(all_prods)
    click.echo('Training compression dictionary')
    zdict = dictionary_from_product_list(dc, products, samples_per_product=50)
    click.echo('..done')

    click.echo('Getting dataset counts')
    counts = {p.name: count
              for p, count in dc.index.datasets.count_by_product(product=[p for p in products])}

    n_total = 0

    for p, c in counts.items():
        click.echo('..{}: {:8,d}'.format(p, c))
        n_total += c

    # TODO: check for overwrite
    cache = dscache.create_cache(output, zdict=zdict, truncate=True)

    conn = db_connect(cfg=env)

    def all_dss(products, conn):
        for p in products:
            yield from map(raw2ds, raw_dataset_stream(p, conn))

    dss = all_dss(products, conn)
    dss = cache.tee(dss)

    label = 'Processing ({:8,d})'.format(n_total)
    with click.progressbar(dss, label=label, length=n_total) as dss:
        for ds in dss:
            pass

    cache.sync()


if __name__ == '__main__':
    cli()
