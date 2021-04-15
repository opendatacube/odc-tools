import click
import json

from ._cli_common import main

from odc.aio import S3Fetcher
from datacube.index.eo3 import prep_eo3
from odc.index.stac import stac_transform
from odc.index import product_from_yaml
from odc.dscache import create_cache
from datacube.model import Dataset


def bytes2ds_doc(data):
    if isinstance(data, bytes):
        data = data.decode("utf-8")
    stac_doc = json.loads(data)
    eo3_doc = stac_transform(stac_doc)
    ds_doc = prep_eo3(eo3_doc)
    return ds_doc


def blob2ds(blob, product):
    doc = bytes2ds_doc(blob.data)
    return Dataset(product, doc, uris=[blob.url])


def s3_fetch_dss(base, product, glob="*.json", s3=None):
    if s3 is None:
        s3 = S3Fetcher(aws_unsigned=True)
    blobs = s3(o.url for o in s3.find(base, glob=glob))
    dss = (blob2ds(b, product) for b in blobs)
    return dss


@main.command("generate-cache")
@click.argument("product", type=str)
@click.argument("input_prefix", type=str)
@click.argument("location", type=str)
@click.option("--verbose", "-v", is_flag=True, help="Be verbose")
def cli(product, input_prefix, location, verbose):
    """
    Dump stats data into a cache file.

    Note: The input bucket must be public otherwise the data can not be listed.
    """

    product = product_from_yaml(product)
    dss = s3_fetch_dss(input_prefix, product, glob="*.json")
    cache = create_cache(f"{product.name}.db")

    if verbose:
        print(f"Writing {location}/{product.name}.db")

    cache = create_cache(f"{location}/{product.name}.db")
    cache.bulk_save(dss)
    if verbose:
        print(f"Found {cache.count:,d} datasets")


if __name__ == "__main__":
    cli()
