import click
import itertools
from ._cli_common import main

from odc.dscache import create_cache
from ._stac_fetch import s3_fetch_dss


@main.command("generate-cache")
@click.argument("input_glob", type=str)
@click.argument("location", type=str)
@click.option("--verbose", "-v", is_flag=True, help="Be verbose")
def cli(input_glob, location, verbose):
    """
    Dump stats data into a cache file.

    Note: The input bucket must be public otherwise the data can not be listed.
    """

    # Look ahead to get product
    dss = s3_fetch_dss(input_glob)
    ds0 = next(dss)
    product = ds0.type
    dss = itertools.chain(iter([ds0]), dss)

    cache = create_cache(f"{product.name}.db")

    if verbose:
        print(f"Writing {location}/{product.name}.db")

    cache = create_cache(f"{location}/{product.name}.db")
    cache.bulk_save(dss)
    if verbose:
        print(f"Found {cache.count:,d} datasets")


if __name__ == "__main__":
    cli()
