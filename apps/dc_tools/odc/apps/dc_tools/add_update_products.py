"""Add or update a list of ODC products. Intended to be used to
systematically maintain a CSV of products and synchronise it with
a database"""

import logging
import sys
from collections import Counter, namedtuple
from csv import DictReader
from typing import Any, Dict, List, Optional, Generator, Tuple

import click
import datacube
import fsspec
import yaml
from datacube import Datacube
from odc.apps.dc_tools.utils import (
    update_if_exists_flag,
    statsd_gauge_reporting,
    statsd_setting,
)

Product = namedtuple("Product", ["name", "doc"])

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s: %(levelname)s: %(message)s",
    datefmt="%m/%d/%Y %I:%M:%S",
)


def _get_product(product_path: str) -> List[Dict[str, Any]]:
    """Returns yaml document"""
    try:
        with fsspec.open(product_path, mode="r") as f:
            return list(yaml.safe_load_all(f))
    except Exception:  # pylint:disable=broad-except
        logging.exception("Failed to get document from %s", product_path)
        return []


def _parse_csv(csv_path: str) -> Generator[Product, None, None]:
    """Parses the CSV and returns a dict of name: yaml_file_path"""

    with fsspec.open(csv_path, mode="r") as f:
        reader = DictReader(f)
        for row in reader:
            names = row["product"].split(";")
            content = _get_product(row["definition"])

            # Do some QA
            fail = False
            # Only return value if we find contents in the document
            if content is None:
                fail = True

            # Check we have the same number of names as content
            if len(names) != len(content):
                logging.error(
                    "%s product names and %s documents found. This is different!",
                    len(names),
                    len(content),
                )
                fail = True

            # Check we have the same names as are in the product definitions
            content_names = [d["name"] for d in content]
            if Counter(content_names) != Counter(names):
                logging.error("%s is not the same as %s", names, content_names)
                fail = True

            if fail:
                yield Product(row["product"], None)
                continue

            # There's only one name in names, so yield it
            if len(names) == 1:
                yield Product(names[0], content[0])
            else:
                # Handle multiple documents in a single file
                for doc in content:
                    # Since we checked all the names, we can do this safely
                    yield Product(doc["name"], doc)


def add_update_products(
    dc: Datacube, csv_path: str, update_if_exists: Optional[bool] = False
) -> Tuple[int, int, int]:
    # Parse csv file
    new_products = list(_parse_csv(csv_path))
    logging.info("Found %s products in the CSV %s", len(new_products), csv_path)

    # List existing products
    products = dc.list_products(with_pandas=False)
    existing_names = [product["name"] for product in products]
    logging.info("Found %s products in the Datacube", len(existing_names))

    added, updated, failed = 0, 0, 0

    for product in new_products:
        if product.doc is None:
            failed += 1
            continue
        # Add new products
        try:
            if product.name not in existing_names:
                dc.index.products.add_document(product.doc)
                added += 1
                logging.info("Added product %s", product.name)
            # Update existing products, if required
            elif update_if_exists:
                dc.index.products.update_document(
                    product.doc, allow_unsafe_updates=True
                )
                updated += 1
                logging.info("Updated product %s", product.name)
        except Exception:  # pylint:disable=broad-except
            failed += 1
            logging.exception("Failed to add/update product %s", product.name)

    # Return results
    return added, updated, failed


@click.command("dc-sync-products")
@click.argument("csv-path", nargs=1)
@update_if_exists_flag
@statsd_setting
def cli(csv_path: str, update_if_exists: bool, statsd_setting: str):
    # Check we can connect to the Datacube
    dc = datacube.Datacube(app="add_update_products")
    logging.info(
        "Starting up: connected to Datacube, and update-if-exists is: %s",
        update_if_exists,
    )

    # TODO: Add in some QA/QC checks
    added, updated, failed = add_update_products(dc, csv_path, update_if_exists)

    print(f"Added: {added}, Updated: {updated} and Failed: {failed}")
    if statsd_setting:
        statsd_gauge_reporting(
            added, ["app: add_update_products", "action:added"], statsd_setting
        )
        statsd_gauge_reporting(
            failed, ["app: add_update_products", "action:failed"], statsd_setting
        )
        statsd_gauge_reporting(
            failed, ["app: add_update_products", "action:updated"], statsd_setting
        )

    # If nothing failed then this exists with success code 0
    sys.exit(failed)


if __name__ == "__main__":
    cli()
