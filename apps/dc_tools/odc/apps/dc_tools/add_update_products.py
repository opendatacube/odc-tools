"""Add or update a list of ODC products. Intended to be used to
systematically maintain a CSV of products and synchronise it with
a database"""


import logging
from csv import DictReader
from typing import Optional

import click
import datacube
import fsspec
import yaml
from datacube import Datacube
from odc.apps.dc_tools.utils import update_if_exists

logging.basicConfig(level=logging.INFO, format='%(asctime)s: %(levelname)s: %(message)s', datefmt='%m/%d/%Y %I:%M:%S')


def _parse_csv(csv_path: str):
    with fsspec.open(csv_path, mode="r") as f:
        reader = DictReader(f)
        for row in reader:
            yield row


def _get_product(product_path: str):
    with fsspec.open(product_path, mode="r") as f:
        return yaml.safe_load(f)


def add_update_products(
    dc: Datacube, csv_path: str, update_if_exists: Optional[bool] = False
):
    # Parse csv file
    names_definitions = _parse_csv(csv_path)
    new_products = {row["product"]: row["definition"] for row in names_definitions}
    logging.info(f"Found {len(new_products.keys())} products in the CSV {csv_path}")

    # List existing products
    products = dc.list_products(with_pandas=False)
    existing_names = [product["name"] for product in products]
    logging.info(f"Found {len(existing_names)} products in the Datacube")

    added, updated, failed = 0, 0, 0

    for name in new_products.keys():
        # Add new products
        try:
            if name not in existing_names:
                product_def = _get_product(new_products[name])
                dc.index.products.add_document(product_def)
                added += 1
                logging.info(f"Added product {name}")
            # Update existing products, if required
            elif update_if_exists:
                product_def = _get_product(new_products[name])
                dc.index.products.update_document(
                    product_def, allow_unsafe_updates=True
                )
                updated += 1
                logging.info(f"Updated product {name}")
        except Exception as e:
            failed += 1
            logging.error(f"Failed to add/update product {name} with exception: {e}")

    # Return results
    return added, updated, failed


@click.command("dc-sync-products")
@click.argument("csv-path", nargs=1)
@update_if_exists
def cli(csv_path: str, update_if_exists: bool):
    # Check we can connect to the Datacube
    dc = datacube.Datacube(app="add_update_products")
    logging.info(f"Starting up: connected to Datacube, and update-if-exists is {update_if_exists}")

    # TODO: Add in some QA/QC checks
    added, updated, failed = add_update_products(dc, csv_path, update_if_exists)

    logging.info(f"Added: {added}, Updated: {updated} and Failed: {failed}")


if __name__ == "__main__":
    cli()
