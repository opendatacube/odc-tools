import click
import sys
from odc.io.text import click_range2d
from ._cli_common import main
from .utils import fuse_products, fuse_ds
from odc.index import ordered_dss, dataset_count
from itertools import groupby


@main.command("save-tasks")
@click.option(
    "--grid",
    type=str,
    help=(
        "Grid name or spec: au-{10|20|30|60},africa-{10|20|30|60}, albers-au-25 (legacy one)"
        "'crs;pixel_resolution;shape_in_pixels'"
    ),
    prompt="""Enter GridSpec
 one of au-{10|20|30|60}, africa-{10|20|30|60}, albers_au_25 (legacy one)
 or custom like 'epsg:3857;30;5000' (30m pixels 5,000 per side in epsg:3857)
 >""",
    default=None,
)
@click.option(
    "--year",
    type=int,
    help="Only extract datasets for a given year. This is a shortcut for --temporal-range=<int>--P1Y",
)
@click.option(
    "--temporal-range",
    type=str,
    help="Only extract datasets for a given time range, Example '2020-05--P1M' month of May 2020",
)
@click.option(
    "--frequency",
    type=str,
    help="Specify temporal binning: annual|annual-fy|semiannual|seasonal|all",
)
@click.option("--env", "-E", type=str, help="Datacube environment name")
@click.option(
    "-z",
    "complevel",
    type=int,
    default=6,
    help="Compression setting for zstandard 1-fast, 9+ good but slow",
)
@click.option(
    "--overwrite", is_flag=True, default=False, help="Overwrite output if it exists"
)
@click.option(
    "--tiles", help='Limit query to tiles example: "0:3,2:4"', callback=click_range2d
)
@click.option(
    "--debug",
    is_flag=True,
    default=False,
    hidden=True,
    help="Dump debug data to pickle",
)
@click.option(
    "--gqa",
    type=float,
    help="Only save datasets that pass `gqa_iterative_mean_xy <= gqa` test",
)
@click.argument("products", type=str, nargs=1)
@click.argument("output", type=str, nargs=1, default="")
def save_tasks(
    grid,
    year,
    temporal_range,
    frequency,
    output,
    products,
    env,
    complevel,
    overwrite=False,
    tiles=None,
    debug=False,
    gqa=None,
):
    """
    Prepare tasks for processing (query db).

    <todo more help goes here>

    \b
    Not yet implemented features:
      - output product config
      - multi-product inputs

    """
    from datacube import Datacube
    from .tasks import SaveTasks
    from .model import DateTimeRange

    if temporal_range is not None and year is not None:
        print("Can only supply one of --year or --temporal_range", file=sys.stderr)
        sys.exit(1)

    if temporal_range is not None:
        try:
            temporal_range = DateTimeRange(temporal_range)
        except ValueError:
            print(f"Failed to parse supplied temporal_range: '{temporal_range}'")
            sys.exit(1)

    if year is not None:
        temporal_range = DateTimeRange.year(year)

    if frequency is not None:
        if frequency not in ("annual", "annual-fy", "semiannual", "seasonal", "all"):
            print(f"Frequency must be one of annual|annual-fy|semiannual|seasonal|all and not '{frequency}'")
            sys.exit(1)

    dc = Datacube(env=env)
    products = products.split("+")
    dss, n_dss, product = _parse_products(dc, products, temporal_range)

    if output == "":
        if temporal_range is not None:
            output = f"{product}_{temporal_range.short}.db"
        else:
            output = f"{product}_all.db"

    try:
        tasks = SaveTasks(
            output, grid, frequency=frequency, overwrite=overwrite, complevel=complevel
        )
    except ValueError as e:
        print(str(e))
        sys.exit(1)

    def on_message(msg):
        print(msg)

    def gqa_predicate(ds):
        return ds.metadata.gqa_iterative_mean_xy <= gqa

    predicate = None
    if gqa is not None:
        predicate = gqa_predicate

    if len(products) != 1: 
        dc = None
        product = None
    try:
        ok = tasks.save(
            dc,
            product,
            temporal_range=temporal_range,
            tiles=tiles,
            predicate=predicate,
            debug=debug,
            msg=on_message,
            dss=dss, 
            n_dss=n_dss,
        )
    except ValueError as e:
        print(str(e))
        sys.exit(2)

    if not ok:
        # exit with error code, failure message was already printed
        sys.exit(3)


def _parse_products(dc, products, temporal_range):
    if len(products) == 1:
        product = products[0]
        dss = None
        n_dss = None
    elif len(products) == 2:
        
        query = {"product": products, "time": (str(temporal_range.start), str(temporal_range.end))}
        dss = ordered_dss(
            dc, key=lambda ds: (ds.center_time, ds.metadata.region_code), **query
        )
        paired_dss = groupby(dss, key=lambda ds: (ds.center_time, ds.metadata.region_code))
        paired_dss = [list(t[1]) for t in paired_dss]
        bad_dss = [x for x in paired_dss if len(x) != 2]
        paired_dss = [x for x in paired_dss if len(x) == 2]
        
        for x in groupby(bad_dss, key=lambda ds: ds[0].type.name):
            name, _iter = x
            print(f"Warning: product {name} has {len(list(_iter))} unpaired datasets")
        
        products = [dc.index.products.get_by_name(product) for product in products]
        fused_product = fuse_products(*products)
        map_fuse_func = lambda x: fuse_ds(*x, product=fused_product)
        dss = map(map_fuse_func, paired_dss)
        product = fused_product.name
        n_dss = dataset_count(dc.index, **query)
    else:
        raise NotImplementedError("Only 1 or 2 products are supported.")

    return dss, n_dss, product
