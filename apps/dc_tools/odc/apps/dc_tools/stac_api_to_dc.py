#!/usr/bin/env python3
"""Index datasets found from an SQS queue into Postgres
"""
import logging
import os
import sys
from typing import Any, Dict, Generator, Optional, Tuple
import concurrent.futures
import click
import pystac
from datacube import Datacube
from datacube.index.hl import Doc2Dataset
from odc.apps.dc_tools.utils import (allow_unsafe, index_update_dataset, limit,
                                     update_if_exists)
from odc.index.stac import stac_transform, stac_transform_absolute
from pystac_client import Client, ItemSearch

logging.basicConfig(level=logging.INFO, format='%(asctime)s: %(levelname)s: %(message)s', datefmt='%m/%d/%Y %I:%M:%S')


def _parse_options(options: Optional[str]) -> Dict[str, Any]:
    parsed_options = {}

    if options is not None:
        for option in options.split("#"):
            try:
                key, value = option.split("=")
                parsed_options[key] = value
            except Exception as e:
                logging.warning(f"Couldn't parse option {option}, format is key=value, exception was {e}")

    return parsed_options


def _guess_location(item: pystac.Item) -> Tuple[str, bool]:
    self_link = None
    asset_link = None
    relative = True

    for link in item.links:
        if link.rel == "self":
            self_link = link.target

        # Override self with canonical
        if link.rel == "canonical":
            self_link = link.target

    for name, asset in item.assets.items():
        if "geotiff" in asset.media_type:
            asset_link = os.path.dirname(asset.href)
            break

    # If the metadata and the document are not on the same path,
    # we need to use absolute links and not relative ones.
    if (self_link and asset_link) and os.path.dirname(self_link) != os.path.dirname(
        asset_link
    ):
        relative = False

    return self_link, relative


def _process_item(item):
    uri, relative = _guess_location(item)
    try:
        metadata = item.to_dict()
        if relative:
            metadata = stac_transform(metadata)
        else:
            metadata = stac_transform_absolute(metadata)
        return (metadata, uri)
    except KeyError as e:
        logging.error(
            f"Failed to handle item with KeyError: '{e}'\n The URI was {uri}"
        )


def get_items(
    search: ItemSearch, threaded: Optional[bool] = False
) -> Generator[Tuple[dict, str, bool], None, None]:
    try:
        items = search.get_all_items()
    except AttributeError:
        items = search.items()

    if threaded:
        with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
            future_to_item = {executor.submit(_process_item, item): item for item in items}
            for future in concurrent.futures.as_completed(future_to_item):
                item = future_to_item[future]
                try:
                    result = future.result()
                except Exception as e:
                    logging.error(f'Failed to handle item {item} with exception {e}')
                else:
                    yield result
    else:
        for item in items:
            yield _process_item(item)


def stac_api_to_odc(
    dc: Datacube,
    update_if_exists: bool,
    config: dict,
    catalog_href: str,
    allow_unsafe_changes: bool = True,
    **kwargs,
) -> Tuple[int, int]:
    doc2ds = Doc2Dataset(dc.index)

    # QA the BBOX
    if config.get("bbox") and len(config["bbox"]) != 4:
        raise ValueError(
            "Bounding box must be of the form lon-min,lat-min,lon-max,lat-max"
        )

    # QA the search
    client = Client.open(catalog_href)
    search = client.search(**config)
    n_items = search.matched()
    if n_items is not None:
        logging.info("Found {} items to index".format(n_items))
        if n_items > 10000:
            logging.warning(
                "More than 10,000 items were returned by your query, which is greater than the API limit"
            )

        if n_items == 0:
            logging.warning("Didn't find any items, finishing.")
            return 0, 0
    else:
        logging.warning("API did not return the number of items.")

    # Get a generator of (stac, uri, relative_uri) tuples
    datasets_uris = get_items(search)

    # Do the indexing of all the things
    success = 0
    failure = 0

    for dataset, uri in datasets_uris:
        try:
            index_update_dataset(
                dataset,
                uri,
                dc,
                doc2ds,
                update_if_exists=update_if_exists,
                allow_unsafe=allow_unsafe_changes,
            )
            success += 1
        except Exception as e:
            logging.warning(f"Failed to handle dataset: {uri} with exception {e}")
            failure += 1

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
@click.option(
    "--bbox",
    type=str,
    default=None,
    help="Comma separated list of bounding box coords, lon-min, lat-min, lon-max, lat-max",
)
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
def cli(limit, update_if_exists, allow_unsafe, catalog_href, collections, bbox, datetime, options):
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

    if limit is not None:
        config["limit"] = limit

    # Do the thing
    dc = Datacube()
    added, failed = stac_api_to_odc(
        dc, update_if_exists, config, catalog_href, allow_unsafe_changes=allow_unsafe
    )

    print(f"Added {added} Datasets, failed {failed} Datasets")

    if failed > 0:
        sys.exit(failed)


if __name__ == "__main__":
    cli()
