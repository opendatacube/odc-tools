#!/usr/bin/env python3
"""Index datasets found from an SQS queue into Postgres
"""
import logging
import os
import sys
from typing import Any, Dict, Generator, Optional, Tuple

import click
import pystac
from datacube import Datacube
from datacube.index.hl import Doc2Dataset
from odc.apps.dc_tools.utils import (
    allow_unsafe,
    index_update_dataset,
    limit,
    update_if_exists,
    bbox,
)
from ._stac import stac_transform, stac_transform_absolute
from pystac.item import Item
from pystac_client import Client

logging.basicConfig(
    level=logging.WARNING,
    format="%(asctime)s: %(levelname)s: %(message)s",
    datefmt="%m/%d/%Y %I:%M:%S",
)

import concurrent


def _parse_options(options: Optional[str]) -> Dict[str, Any]:
    parsed_options = {}

    if options is not None:
        for option in options.split("#"):
            try:
                key, value = option.split("=")
                parsed_options[key] = value
            except Exception as e:
                logging.warning(
                    f"Couldn't parse option {option}, format is key=value, exception was {e}"
                )

    return parsed_options


def _guess_location(
    item: pystac.Item, rewrite: Optional[Tuple[str, str]] = None
) -> Tuple[str, bool]:
    self_link = None
    asset_link = None
    relative = True

    for link in item.links:
        if link.rel == "self":
            self_link = link.target

        # Override self with canonical
        if link.rel == "canonical":
            self_link = link.target
            break

    for _, asset in item.assets.items():
        if "geotiff" in asset.media_type:
            asset_link = os.path.dirname(asset.href)
            break

    if rewrite is not None:
        for _, asset in item.assets.items():
            if "geotiff" in asset.media_type:
                asset.href = asset.href.replace(rewrite[0], rewrite[1])

    # If the metadata and the document are not on the same path,
    # we need to use absolute links and not relative ones.
    if (self_link and asset_link) and os.path.dirname(self_link) != os.path.dirname(
        asset_link
    ):
        relative = False

    return self_link, relative


def item_to_meta_uri(
    item: Item, rewrite: Optional[Tuple[str, str]] = None, rename_product: Optional[str] = None
) -> Generator[Tuple[dict, str, bool], None, None]:
    uri, relative = _guess_location(item, rewrite)
    metadata = item.to_dict()
    if rename_product is not None:
        metadata["properties"]["odc:product"] = rename_product

    if relative:
        metadata = stac_transform(metadata)
    else:
        metadata = stac_transform_absolute(metadata)

    return (metadata, uri)


def process_item(
    item: Item,
    dc: Datacube,
    doc2ds: Doc2Dataset,
    update_if_exists: bool,
    allow_unsafe: bool,
    rewrite: Optional[Tuple[str, str]] = None,
    rename_product: Optional[str] = None,
):
    meta, uri = item_to_meta_uri(item, rewrite, rename_product)
    index_update_dataset(
        meta,
        uri,
        dc,
        doc2ds,
        update_if_exists=update_if_exists,
        allow_unsafe=allow_unsafe,
    )


def stac_api_to_odc(
    dc: Datacube,
    update_if_exists: bool,
    config: dict,
    catalog_href: str,
    allow_unsafe: bool = True,
    rewrite: Optional[Tuple[str, str]] = None,
    rename_product: Optional[str] = None,
) -> Tuple[int, int]:
    doc2ds = Doc2Dataset(dc.index)
    client = Client.open(catalog_href)

    search = client.search(**config)
    n_items = search.matched()
    if n_items is not None:
        logging.info("Found {} items to index".format(n_items))
        if n_items == 0:
            logging.warning("Didn't find any items, finishing.")
            return 0, 0
    else:
        logging.warning("API did not return the number of items.")

    # Do the indexing of all the things
    success = 0
    failure = 0

    sys.stdout.write("\rIndexing from STAC API...\n")
    with concurrent.futures.ThreadPoolExecutor(max_workers=50) as executor:
        future_to_item = {
            executor.submit(
                process_item,
                item,
                dc,
                doc2ds,
                update_if_exists=update_if_exists,
                allow_unsafe=allow_unsafe,
                rewrite=rewrite,
                rename_product=rename_product,
            ): item.id
            for item in search.get_all_items()
        }
        for future in concurrent.futures.as_completed(future_to_item):
            item = future_to_item[future]
            try:
                _ = future.result()
                success += 1
                if success % 10 == 0:
                    sys.stdout.write(f"\rAdded {success} datasets...")
            except Exception as e:
                logging.exception(f"Failed to handle item {item} with exception {e}")
                failure += 1
    sys.stdout.write("\r")

    return success, failure


@click.command("stac-to-dc")
@limit
@update_if_exists
@allow_unsafe
@click.option(
    "--catalog-href",
    type=str,
    default="https://earth-search.aws.element84.com/v0/",
    help="URL to the catalog to search",
)
@click.option(
    "--collections",
    type=str,
    default=None,
    help="Comma separated list of collections to search",
)
@bbox
@click.option(
    "--datetime",
    type=str,
    default=None,
    help="Dates to search, either one day or an inclusive range, e.g. 2020-01-01 or 2020-01-01/2020-01-02",
)
@click.option(
    "--options",
    type=str,
    default=None,
    help="Other search terms, as a # separated list, i.e., --options=cloud_cover=0,100#sky=green",
)
@click.option(
    "--rewrite-assets",
    type=str,
    default=None,
    help=(
        "Rewrite asset hrefs, for example, to change from "
        "HTTPS to S3 URIs, --rewrite-assets=https://example.com/,s3://"
    )
)
@click.option(
    "--rename-product",
    type=str,
    default=None,
    help=(
        "Name of product to overwrite collection(s) names, only one product name can overwrite, despite multiple collections "
    )
)
def cli(
    limit,
    update_if_exists,
    allow_unsafe,
    catalog_href,
    collections,
    bbox,
    datetime,
    options,
    rewrite_assets,
    rename_product,
):
    """
    Iterate through STAC items from a STAC API and add them to datacube.
    """
    config = _parse_options(options)
    rewrite = None

    # Format the search terms
    if bbox:
        config["bbox"] = list(map(float, bbox.split(",")))

    if collections:
        config["collections"] = collections.split(",")

    if datetime:
        config["datetime"] = datetime

    # Always set the limit, because some APIs will stop at an arbitrary
    # number if max_items is not None.
    config["max_items"] = limit

    if rewrite_assets is not None:
        rewrite = list(rewrite_assets.split(","))
        if len(rewrite) != 2:
            raise ValueError("Rewrite assets argument needs to be two strings split by ','")

    # Do the thing
    dc = Datacube()
    added, failed = stac_api_to_odc(
        dc, update_if_exists, config, catalog_href, allow_unsafe=allow_unsafe, rewrite=rewrite, rename_product=rename_product
    )

    print(f"Added {added} Datasets, failed {failed} Datasets")

    if failed > 0:
        sys.exit(failed)


if __name__ == "__main__":
    cli()
