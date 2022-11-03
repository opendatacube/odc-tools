#!/usr/bin/env python3
"""Index datasets found from an SQS queue into Postgres
"""
import concurrent
import json
import logging
import os
import sys
from typing import Any, Dict, Generator, Optional, Tuple

import click
import pystac
from datacube import Datacube
from datacube.index.hl import Doc2Dataset
from odc.apps.dc_tools._stac import stac_transform, stac_transform_absolute
from odc.apps.dc_tools.utils import (
    SkippedException,
    allow_unsafe,
    archive_less_mature,
    bbox,
    index_update_dataset,
    limit,
    statsd_gauge_reporting,
    statsd_setting,
    update_if_exists_flag,
    publish_action,
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
    item: Item,
    rewrite: Optional[Tuple[str, str]] = None,
    rename_product: Optional[str] = None,
) -> Generator[Tuple[dict, str, bool], None, None]:
    uri, relative = _guess_location(item, rewrite)
    metadata = item.to_dict()
    if rename_product is not None:
        metadata["properties"]["odc:product"] = rename_product
    stac = metadata

    if relative:
        metadata = stac_transform(metadata)
    else:
        metadata = stac_transform_absolute(metadata)

    return (metadata, uri, stac)


def process_item(
    item: Item,
    dc: Datacube,
    doc2ds: Doc2Dataset,
    update_if_exists: bool,
    allow_unsafe: bool,
    rewrite: Optional[Tuple[str, str]] = None,
    rename_product: Optional[str] = None,
    archive_less_mature: bool = False,
    publish_action: bool = False,
):
    meta, uri, stac = item_to_meta_uri(item, rewrite, rename_product)
    index_update_dataset(
        meta,
        uri,
        dc,
        doc2ds,
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
    rewrite: Optional[Tuple[str, str]] = None,
    rename_product: Optional[str] = None,
    archive_less_mature: bool = False,
    publish_action: Optional[str] = None,
) -> Tuple[int, int, int]:
    doc2ds = Doc2Dataset(dc.index)
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
                doc2ds,
                update_if_exists=update_if_exists,
                allow_unsafe=allow_unsafe,
                rewrite=rewrite,
                rename_product=rename_product,
                archive_less_mature=archive_less_mature,
                publish_action=publish_action,
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
            except SkippedException:
                skipped += 1
            except Exception:  # pylint:disable=broad-except
                logging.exception("Failed to handle item %s", item)
                failure += 1
    sys.stdout.write("\r")

    return success, failure, skipped


@click.command("stac-to-dc")
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
@click.option(
    "--rewrite-assets",
    type=str,
    default=None,
    help=(
        "Rewrite asset hrefs, for example, to change from "
        "HTTPS to S3 URIs, --rewrite-assets=https://example.com/,s3://"
    ),
)
@click.option(
    "--rename-product",
    type=str,
    default=None,
    help=(
        "Name of product to overwrite collection(s) names, "
        "only one product name can overwrite, despite multiple collections "
    ),
)
@archive_less_mature
@publish_action
@statsd_setting
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
    statsd_setting,
    archive_less_mature,
    publish_action,
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
            raise ValueError(
                "Rewrite assets argument needs to be two strings split by ','"
            )

    # Do the thing
    dc = Datacube()
    added, failed, skipped = stac_api_to_odc(
        dc,
        update_if_exists,
        config,
        catalog_href,
        allow_unsafe=allow_unsafe,
        rewrite=rewrite,
        rename_product=rename_product,
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
