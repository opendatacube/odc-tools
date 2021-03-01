#!/usr/bin/env python3
"""Index datasets found from an SQS queue into Postgres
"""
import json
import logging
import os
import sys
from typing import Any, Dict, Generator, Iterable, List, Tuple

import click
from datacube import Datacube
from datacube.index.hl import Doc2Dataset
from datacube.utils import changes
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
    srch: Search, limit: int
) -> Generator[Tuple[dict, str, bool], None, None]:
    if limit:
        items = srch.items(limit=limit)
    else:
        items = srch.items()

    # Workaround bug in STAC Search that doesn't stop at the limit
    count = 0
    for metadata in items.geojson()["features"]:
        count += 1
        if count > limit:
            break
        uri, relative = guess_location(metadata)
        yield (metadata, uri, relative)


def transform_items(
    doc2ds: Doc2Dataset, items: Iterable[Tuple[Dict[str, Any], str, bool]]
) -> Generator[Tuple[dict, str], None, None]:
    for metadata, uri, relative in items:
        try:
            if relative:
                metadata = stac_transform(metadata)
            else:
                metadata = stac_transform_absolute(metadata)
        except KeyError as e:
            logging.error(
                f"Failed to handle item with KeyError: '{e}'\n The URI was {uri}"
            )
            yield None, uri
            continue

        try:
            ds, err = doc2ds(metadata, uri)
        except ValueError as e:
            logging.error(
                f"Exception thrown when trying to create dataset: '{e}'\n The URI was {uri}"
            )
        if ds is not None:
            yield ds, uri
        else:
            logging.error(
                f"Failed to create dataset with error {err}\n The URI was {uri}"
            )
            yield None, uri


def index_update_datasets(
    dc: Datacube, datasets: Tuple[dict, str], update: bool, allow_unsafe: bool
) -> Tuple[int, int]:
    ds_added = 0
    ds_failed = 0

    for dataset, uri in datasets:
        if uri is not None:
            if dataset is not None:
                if update:
                    updates = {}
                    if allow_unsafe:
                        updates = {tuple(): changes.allow_any}
                    dc.index.datasets.update(dataset, updates_allowed=updates)
                else:
                    ds_added += 1
                    dc.index.datasets.add(dataset)
            else:
                ds_failed += 1
        else:
            ds_failed += 1

    return ds_added, ds_failed


def stac_api_to_odc(
    dc: Datacube,
    products: list,
    limit: int,
    update: bool,
    allow_unsafe: bool,
    config: dict,
    **kwargs,
) -> Tuple[int, int]:
    # QA the BBOX
    if config["bbox"]:
        assert (
            len(config["bbox"]) == 4
        ), "Bounding box must be of the form lon-min,lat-min,lon-max,lat-max"

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
    potential_items = get_items(srch, limit)

    # Get a generator of (dataset, uri)
    doc2ds = Doc2Dataset(dc.index, **kwargs)
    datasets = transform_items(doc2ds, potential_items)

    # Do the indexing of all the things
    return index_update_datasets(dc, datasets, update, allow_unsafe)


@click.command("stac-to-dc")
@click.option(
    "--limit",
    default=None,
    type=int,
    help="Stop indexing after n datasets have been indexed.",
)
@click.option(
    "--update",
    is_flag=True,
    default=False,
    help="If set, update instead of add datasets",
)
@click.option(
    "--allow-unsafe",
    is_flag=True,
    default=False,
    help="Allow unsafe changes to a dataset. Take care!",
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
@click.argument("product", type=str, nargs=1)
def cli(
    limit,
    update,
    allow_unsafe,
    collections,
    bbox,
    datetime,
    product,
):
    """
    Iterate through STAC items from a STAC API and add them to datacube
    Note that you need to set the STAC_API_URL environment variable to
    something like https://earth-search.aws.element84.com/v0/
    """

    candidate_products = product.split()

    config = {
        "datetime": datetime,
        "bbox": bbox,
        "collections": collections,
    }

    # Format the search terms
    if bbox:
        config["bbox"] = list(map(float, config["bbox"].split(",")))

    if config["collections"]:
        config["collections"] = config["collections"].split(",")

    # Do the thing
    dc = Datacube()
    added, failed = stac_api_to_odc(
        dc, candidate_products, limit, update, allow_unsafe, config
    )

    print(f"Added {added} Datasets, failed {failed} Datasets")

    if failed > 0:
        sys.exit(failed)


if __name__ == "__main__":
    cli()
