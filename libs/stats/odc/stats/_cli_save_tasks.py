import json

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
@click.option(
    "--dataset-filter",
    type=str,
    default=None,
    help='Filter to apply on datasets - {"collection_category": "T1"}'
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
    dataset_filter,
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

    filter = {}
    if dataset_filter:
        filter = json.loads(dataset_filter)

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
    if len(products) == 1:
        product = products[0]
        dss = None
        n_dss = None
    else:
        dss, n_dss, product, error_logger = _parse_products(dc, products, filter, temporal_range)

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

    try:
        ok = tasks.save(
            dc,
            product,
            dataset_filter=filter,
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

    if len(products) != 1:
        for product, count in error_logger.missing_counts.items():
            print(f"Product {product} has {count} missing datasets.")

    if not ok:
        # exit with error code, failure message was already printed
        sys.exit(3)


def _parse_products(dc, products, dataset_filter, temporal_range):

    query = dict(product=products, **dataset_filter)

    # TODO: find time range
    if temporal_range:
        query.update(temporal_range.dc_query(pad=0.6))
        dss = ordered_dss(dc, key=lambda ds: (ds.center_time, ds.metadata.region_code), **query)
        n_dss = min(dataset_count(dc.index, time=query["time"], product=product) for product in products)
    else:
        dss = dc.find_datasets(**query)
        dss.sort(key=lambda ds: (ds.center_time, ds.metadata.region_code))
        n_dss = min(dataset_count(dc.index, product=product) for product in products)

    paired_dss = groupby(dss, key=lambda ds: (ds.center_time, ds.metadata.region_code))
    error_logger = ErrorLogger(products)
    paired_dss = error_logger.filter(paired_dss)

    products = [dc.index.products.get_by_name(product) for product in products]
    fused_product = fuse_products(*products)
    map_fuse_func = lambda x: fuse_ds(*x, product=fused_product)
    dss = map(map_fuse_func, paired_dss)
    product = fused_product.name

    return dss, n_dss, product, error_logger


class ErrorLogger:

    def __init__(self, products):
        self.products = products
        self.missing_counts = dict((p, 0) for p in products)

    def append(self, ds_group):
        product_group = tuple(ds.type.name for ds in ds_group)
        for product in self.products:
            if product not in product_group:
                self.missing_counts[product] += 1

    def check(self, ds_group):
        return len(ds_group) == len(self.products)

    def filter(self, groups):
        for _, ds_group in groups:
            ds_group = tuple(ds_group)
            if not self.check(ds_group):
                self.append(ds_group)
            else:
                yield ds_group

