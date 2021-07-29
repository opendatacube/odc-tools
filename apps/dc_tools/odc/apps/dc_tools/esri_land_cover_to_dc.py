#!/usr/bin/env python3
"""
Index ESRI Land Cover automatically.
"""
import logging
import sys
from typing import Tuple

import click
from datacube import Datacube
from datacube.index.hl import Doc2Dataset
from datacube.utils import read_documents
from odc.apps.dc_tools.stac_api_to_dc import get_items
from odc.apps.dc_tools.utils import (index_update_dataset, limit,
                                     update_if_exists)
from pystac_client import Client

logging.basicConfig(level=logging.INFO, format='%(asctime)s: %(levelname)s: %(message)s', datefmt='%m/%d/%Y %I:%M:%S')


ESRI_LANDCOVER_PRODUCT = (
    "https://raw.githubusercontent.com/opendatacube/"
    "datacube-dataset-config/master/products/io_lulc.odc-product.yaml"
)
MICROSOFT_PC_URL = "https://planetarycomputer.microsoft.com/api/stac/v1/"


def add_esri_lc_product(dc: Datacube):
    """ Add the ESRI Land Cover product definition"""
    for path, doc in read_documents(ESRI_LANDCOVER_PRODUCT):
        dc.index.products.add_document(doc)
    logging.info("Product definition added.")


def esri_lc_to_dc(dc: Datacube, limit: int, update: bool) -> Tuple[int, int]:
    doc2ds = Doc2Dataset(dc.index)
    # Get a generator of (dataset_json, uri)
    client = Client.open(MICROSOFT_PC_URL)
    search = client.search(limit=limit, collections="io-lulc")

    # Do the indexing of all the things
    success = 0
    failure = 0

    for metadata, uri in get_items(search, limit):
        metadata["properties"]["odc:region_code"] = metadata["properties"]["io:supercell_id"]
        try:
            index_update_dataset(
                metadata, uri, dc, doc2ds, update_if_exists=update, allow_unsafe=True
            )
            success += 1
        except Exception as e:
            logging.warning(f"Failed to index {uri} with exception {e}")
            failure += 1

    return success, failure


@click.command("esri-lc-to-dc")
@limit
@update_if_exists
@click.option(
    "--add-product",
    is_flag=True,
    default=False,
    help="If set, add the product too",
)
def cli(limit, update_if_exists, add_product):
    """
    Add all of the ESRI Land Cover scenes to an ODC Database.
    Optionally add the product definition with `--add-product`.
    Default is to index all 700 scenes, but you can limit it for testing purposes with `--limit 1`.

    Add all the scenes in a new database with `esri-lc-to-dc --add-product`.
    """

    dc = Datacube()

    if add_product:
        add_esri_lc_product(dc)

    added, failed = esri_lc_to_dc(dc, limit, update_if_exists)

    print(f"Added {added} Datasets, failed {failed} Datasets")

    if failed > 0:
        sys.exit(failed)


if __name__ == "__main__":
    cli()
