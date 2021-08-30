import click
from threading import Thread
import queue
import datacube
from odc import dscache
from odc.dscache.tools import db_connect, raw_dataset_stream, mk_raw2ds
from odc.dscache.tools import dictionary_from_product_list
from odc.dscache.tools.tiling import parse_gridspec
from odc.index import bin_dataset_stream, ordered_dss, dataset_count


EOS = object()


def qmap(proc, q, eos_marker=None):
    """Converts queue to an iterator.

    For every `item` in the `q` that is not `eos_marker`, `yield proc(item)`
    """
    while True:
        item = q.get(block=True)
        try:
            if item is eos_marker:
                break

            yield proc(item)
        finally:
            q.task_done()


@click.command("slurpy")
@click.option("--env", "-E", type=str, help="Datacube environment name")
@click.option(
    "-z",
    "complevel",
    type=int,
    default=6,
    help="Compression setting for zstandard 1-fast, 9+ good but slow",
)
@click.option(
    "--grid",
    type=str,
    help=(
        "Grid spec or name 'crs;pixel_resolution;shape_in_pixels',"
        "albers_au_25, africa_{10|20|30|60}"
    ),
    default=None,
)
@click.option("--year", type=int, help="Only extract datasets for a given year")
@click.argument("output", type=str, nargs=1)
@click.argument("products", type=str, nargs=-1)
def cli(env, grid, year, output, products, complevel):
    """Extract product(s) to an on disk cache.

    Optionally tile datasets into a grid while extracting (see --grid option)
    """
    # pylint: disable=too-many-locals,too-many-statements

    if len(products) == 0:
        click.echo("Have to supply at least one product")
        raise click.Abort()

    dc = datacube.Datacube(env=env)
    all_prods = {p.name: p for p in dc.index.products.get_all()}

    if len(products) == 1 and products[0].lower() in (":all:", "*"):
        click.echo("Will read all products")
        products = list(all_prods)

    for p in products:
        if p not in all_prods:
            click.echo("No such product found: %s" % p)
            raise click.Abort()

    query = {}
    if year is not None:
        query.update(time=f"{year}")

    click.echo("Getting dataset counts")
    counts = {p: dataset_count(dc.index, product=p, **query) for p in products}

    n_total = 0
    for p, c in counts.items():
        click.echo("..{}: {:8,d}".format(p, c))
        n_total += c

    if n_total == 0:
        click.echo("No datasets found")
        raise click.Abort()

    click.echo("Training compression dictionary")
    zdict = dictionary_from_product_list(
        dc, products, samples_per_product=50, query=query
    )
    click.echo("..done")

    # TODO: check for overwrite
    cache = dscache.create_cache(
        output, zdict=zdict, complevel=complevel, truncate=True
    )

    raw2ds = mk_raw2ds(all_prods)

    def db_task(products, conn, q):
        for p in products:
            if len(query) == 0:
                dss = map(raw2ds, raw_dataset_stream(p, conn))
            else:
                dss = ordered_dss(dc, product=p, **query)

            for ds in dss:
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
        dss = bin_dataset_stream(gs, dss, cells)

    label = "Processing ({:8,d})".format(n_total)
    with click.progressbar(dss, label=label, length=n_total) as dss:
        for _ in dss:
            pass

    if grid is not None:
        click.echo("Total bins: {:d}".format(len(cells)))

        with click.progressbar(
            cells.values(), length=len(cells), label="Saving"
        ) as groups:
            for group in groups:
                cache.add_grid_tile(group_prefix, group.idx, group.dss)

    db_thread.join()
    cache.close()


if __name__ == "__main__":
    cli()
