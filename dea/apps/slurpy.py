import click
import datacube
import dscache
from dscache.tools import db_connect, raw_dataset_stream, mk_raw2ds
from dscache.tools import dictionary_from_product_list
from dscache.tools.profiling import ds_stream_test_func


@click.command('slurpy')
@click.option('--env', type=str, help='Datacube environment name')
@click.argument('output', type=str, nargs=1)
@click.argument('products', type=str, nargs=-1)
def slurpy(env, output, products):

    dc = datacube.Datacube(env=env)
    all_prods = {p.name: p
                 for p in dc.index.products.get_all()}

    for p in products:
        if p not in all_prods:
            click.echo('No such product found: %s' % p)
            raise click.Abort()

    raw2ds = mk_raw2ds(all_prods)
    zdict = dictionary_from_product_list(dc, products, samples_per_product=50)

    # TODO: check for overwrite
    cache = dscache.create_cache(output, zdict=zdict, truncate=True)

    conn = db_connect(cfg=env)

    for p in products:
        dss = map(raw2ds, raw_dataset_stream(p, conn))
        dss = cache.tee(dss)

        rr = ds_stream_test_func(dss, lambda ds: ds.id)
        print(rr.text)

    cache.sync()


if __name__ == '__main__':
    slurpy()
