#!/usr/bin/env python3
"""Index datasets found from an SQS queue into Postgres"""
import concurrent
import json
import logging
import sys
from typing import Any, Dict, Generator, Optional, Tuple

import click
from datacube import Datacube
from datacube.model import Dataset
from datacube.ui.click import environment_option, pass_config
from odc.stac.eo3 import stac2ds

from odc.apps.dc_tools.utils import (
    SkippedException,
    allow_unsafe,
    archive_less_mature,
    bbox,
    index_update_dataset,
    limit,
    publish_action,
    rename_product,
    statsd_gauge_reporting,
    statsd_setting,
    update_if_exists_flag,
)
from pystac.item import Item
from pystac_client import Client

logging.basicConfig(
    level=logging.WARNING,
    format="%(asctime)s: %(levelname)s: %(message)s",
    datefmt="%m/%d/%Y %I:%M:%S",
)


def _parse_options(options: Optional[str]) -> Dict[str, Any]:
    parsed_options = {}

    if options is not None:
        for option in options.split("#"):
            try:
                key, value = option.split("=")

                try:
                    value = json.loads(value)
                except Exception:  # pylint:disable=broad-except
                    logging.warning(
                        "Failed to handle value %s for key %s as JSON, using str",
                        value,
                        key,
                    )
                parsed_options[key] = value
            except Exception:  # pylint:disable=broad-except
                logging.warning(
                    "Couldn't parse option %s format is key=value",
                    option,
                    exc_info=True,
                )

    return parsed_options


def item_to_meta_uri(
    item: Item,
    dc: Datacube,
    rename_product: Optional[str] = None,
    url_string_replace: tuple[str, str] | None = None,
) -> Generator[Tuple[Dataset, str, bool], None, None]:
    for link in item.links:
        if link.rel == "self":
            uri = link.target

        # Override self with canonical
        if link.rel == "canonical":
            uri = link.target
            break

    if rename_product is not None:
        item.properties["odc:product"] = rename_product

    # If we need to modify URLs, do it for the main URL and the asset links
    if url_string_replace is not None:
        old_url, new_url = url_string_replace

        uri = uri.replace(old_url, new_url)

        for asset in item.assets.values():
            asset.href = asset.href.replace(old_url, new_url)

    # Try to the Datacube product for the dataset
    product_name = item.properties.get("odc:product", item.collection_id)
    product_name_sanitised = product_name.replace("-", "_")
    product = dc.index.products.get_by_name(product_name_sanitised)

    if product is None:
        logging.warning(
            "Couldn't find matching product for product name: %s",
            product_name_sanitised,
        )
        raise SkippedException(
            f"Couldn't find matching product for product name: {product_name_sanitised}"
        )

    # Convert the STAC Item to a Dataset
    dataset = next(stac2ds([item]))
    # And assign the product ID
    dataset.product = product

    return (dataset, uri, item.to_dict())


def process_item(
    item: Item,
    dc: Datacube,
    update_if_exists: bool,
    allow_unsafe: bool,
    rename_product: Optional[str] = None,
    url_string_replace: tuple[str, str] | None = None,
    archive_less_mature: int | None = None,
    publish_action: bool = False,
) -> None:
    dataset, uri, stac = item_to_meta_uri(item, dc, rename_product, url_string_replace)
    index_update_dataset(
        dataset,
        uri,
        dc,
        None,
        update_if_exists=update_if_exists,
        allow_unsafe=allow_unsafe,
        archive_less_mature=archive_less_mature,
        publish_action=publish_action,
        stac_doc=stac,
    )


def stac_api_to_odc(
    dc: Datacube,
    update_if_exists: bool,
    config: dict,
    catalog_href: str,
    allow_unsafe: bool = True,
    rename_product: Optional[str] = None,
    url_string_replace: tuple[str, str] | None = None,
    archive_less_mature: int | None = None,
    publish_action: Optional[str] = None,
) -> Tuple[int, int, int]:
    client = Client.open(catalog_href)

    search = client.search(**config)
    n_items = search.matched()
    if n_items is not None:
        logging.info("Found %s items to index", n_items)
        if n_items == 0:
            logging.warning("Didn't find any items, finishing.")
            return 0, 0, 0
    else:
        logging.warning("API did not return the number of items.")

    # Do the indexing of all the things
    success = 0
    failure = 0
    skipped = 0

    sys.stdout.write("\rIndexing from STAC API...\n")
    with concurrent.futures.ThreadPoolExecutor(max_workers=50) as executor:
        future_to_item = {
            executor.submit(
                process_item,
                item,
                dc,
                update_if_exists=update_if_exists,
                allow_unsafe=allow_unsafe,
                rename_product=rename_product,
                url_string_replace=url_string_replace,
                archive_less_mature=archive_less_mature,
                publish_action=publish_action,
            ): item.id
            for item in search.items()
        }
        for future in concurrent.futures.as_completed(future_to_item):
            item = future_to_item[future]
            try:
                _ = future.result()
                success += 1
                if success % 10 == 0:
                    sys.stdout.write(f"\rAdded {success} datasets...")
            except SkippedException:
                skipped += 1
            except Exception:  # pylint:disable=broad-except
                logging.exception("Failed to handle item %s", item)
                failure += 1
    sys.stdout.write("\r")

    return success, failure, skipped


@click.command("stac-to-dc")
@environment_option
@pass_config
@limit
@update_if_exists_flag
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
@rename_product
@click.option(
    "--url-string-replace",
    type=str,
    default=None,
    help="Replace a string in the STAC API URLs, e.g., 'https://stac.example.com,s3://stac.example.org'",
)
@archive_less_mature
@publish_action
@statsd_setting
def cli(
    cfg_env,
    limit,
    update_if_exists,
    allow_unsafe,
    catalog_href,
    collections,
    bbox,
    datetime,
    options,
    rename_product,
    url_string_replace,
    archive_less_mature,
    publish_action,
    statsd_setting,
) -> None:
    """
    Iterate through STAC items from a STAC API and add them to datacube.
    """
    config = _parse_options(options)

    # Format the search terms
    if bbox:
        config["bbox"] = list(map(float, bbox.split(",")))

    if collections:
        config["collections"] = collections.split(",")

    if datetime:
        config["datetime"] = datetime

    if url_string_replace:
        url_string_replace_tuple = tuple(url_string_replace.split(","))
        if len(url_string_replace_tuple) != 2:
            raise ValueError(
                "url_string_replace must be two strings separated by a comma"
            )

    # Always set the limit, because some APIs will stop at an arbitrary
    # number if max_items is not None.
    config["max_items"] = limit

    # Do the thing
    dc = Datacube(env=cfg_env)
    added, failed, skipped = stac_api_to_odc(
        dc,
        update_if_exists,
        config,
        catalog_href,
        allow_unsafe=allow_unsafe,
        rename_product=rename_product,
        url_string_replace=url_string_replace_tuple,
        archive_less_mature=archive_less_mature,
        publish_action=publish_action,
    )

    print(
        f"Added {added} Datasets, failed {failed} Datasets, skipped {skipped} Datasets"
    )
    if statsd_setting:
        statsd_gauge_reporting(
            added, ["app:stac_api_to_dc", "action:added"], statsd_setting
        )
        statsd_gauge_reporting(
            failed, ["app:stac_api_to_dc", "action:failed"], statsd_setting
        )
        statsd_gauge_reporting(
            skipped, ["app:stac_api_to_dc", "action:skipped"], statsd_setting
        )

    if failed > 0:
        sys.exit(failed)


if __name__ == "__main__":
    cli()
