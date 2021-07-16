#!/usr/bin/env python3
"""Index datasets found from an SQS queue into Postgres
"""
import logging
import os
import sys
from typing import Generator, Optional, Tuple

import click
from datacube import Datacube
from datacube.index.hl import Doc2Dataset
from odc.apps.dc_tools.utils import (
    allow_unsafe,
    index_update_dataset,
    limit,
    update_if_exists,
)
from odc.index.stac import stac_transform, stac_transform_absolute
from satsearch import Search


def guess_location(metadata: dict) -> Tuple[str, bool]:
    self_link = None
    asset_link = None
    relative = True

    for link in metadata.get("links"):
        rel = link.get("rel")
        if rel and rel == "self":
            self_link = link.get("href")

    if metadata.get("assets"):
        for asset in metadata["assets"].values():
            if asset.get("type") in [
                "image/tiff; application=geotiff; profile=cloud-optimized",
                "image/tiff; application=geotiff",
            ]:
                asset_link = os.path.dirname(asset["href"])

    # If the metadata and the document are not on the same path,
    # we need to use absolute links and not relative ones.
    if (self_link and asset_link) and os.path.dirname(self_link) != os.path.dirname(
        asset_link
    ):
        relative = False

    return self_link, relative


def get_items(
    srch: Search, limit: Optional[int]
) -> Generator[Tuple[dict, str, bool], None, None]:
    if limit:
        items = srch.items(limit=limit)
    else:
        items = srch.items()

    # Workaround bug in STAC Search that doesn't stop at the limit
    for count, metadata in enumerate(items.geojson()["features"]):
        # Stop at the limit if it's set
        if (limit is not None) and (count >= limit):
            break
        uri, relative = guess_location(metadata)
        try:
            if relative:
                metadata = stac_transform(metadata)
            else:
                metadata = stac_transform_absolute(metadata)
        except KeyError as e:
            logging.error(
                f"Failed to handle item with KeyError: '{e}'\n The URI was {uri}"
            )
            continue

        yield (metadata, uri)


def stac_api_to_odc(
    dc: Datacube,
    limit: int,
    update_if_exists: bool,
    config: dict,
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
    srch = Search().search(**config)
    n_items = srch.found()
    logging.info("Found {} items to index".format(n_items))
    if n_items > 10000:
        logging.warning(
            "More than 10,000 items were returned by your query, which is greater than the API limit"
        )

    if n_items == 0:
        logging.warning("Didn't find any items, finishing.")
        return 0, 0

    # Get a generator of (stac, uri, relative_uri) tuples
    datasets = get_items(srch, limit)

    # Do the indexing of all the things
    success = 0
    failure = 0

    for dataset, uri in datasets:
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
def cli(limit, update_if_exists, allow_unsafe, collections, bbox, datetime):
    """
    Iterate through STAC items from a STAC API and add them to datacube
    Note that you need to set the STAC_API_URL environment variable to
    something like https://earth-search.aws.element84.com/v0/
    """

    config = {}

    # Format the search terms
    if bbox:
        config["bbox"] = list(map(float, bbox.split(",")))

    if collections:
        config["collections"] = collections.split(",")

    if datetime:
        config["datetime"] = datetime

    # Do the thing
    dc = Datacube()
    added, failed = stac_api_to_odc(
        dc, limit, update_if_exists, config, allow_unsafe_changes=allow_unsafe
    )

    print(f"Added {added} Datasets, failed {failed} Datasets")

    if failed > 0:
        sys.exit(failed)


if __name__ == "__main__":
    cli()
