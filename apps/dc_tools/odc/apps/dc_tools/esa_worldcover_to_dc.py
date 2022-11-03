#!/usr/bin/env python3
"""
Index the ESA Worldcover data automatically.
"""
import concurrent.futures
import logging
import sys
from math import ceil, floor
from typing import Tuple

import click
import pystac
import rasterio
from datacube import Datacube
from datacube.index.hl import Doc2Dataset
from datacube.utils import read_documents
from odc.apps.dc_tools.utils import (
    bbox,
    index_update_dataset,
    limit,
    update_if_exists_flag,
    archive_less_mature,
    statsd_gauge_reporting,
    statsd_setting,
    publish_action,
)
from rio_stac import create_stac_item

from ._stac import stac_transform

PRODUCT = (
    "https://raw.githubusercontent.com/opendatacube/"
    "datacube-dataset-config/master/products/esa_worldcover.odc-product.yaml"
)

# URIs need north/south, which is N00 and east/west, which is E000
URI_TEMPLATE = (
    "https://esa-worldcover.s3.eu-central-1.amazonaws.com/"
    "v100/2020/map/ESA_WorldCover_10m_2020_v100_{ns}{ew}_Map.tif"
)


def _unpack_bbox(bounding_box: str) -> Tuple[int, int, int, int]:
    """Set up a left, bottom, right, top bbox with 3 degree offset"""
    left = floor(bounding_box[0])
    bottom = floor(bounding_box[1])
    right = ceil(bounding_box[2])
    top = ceil(bounding_box[3])

    left_offset = left % 3
    bottom_offset = bottom % 3
    right_offset = right % 3
    top_offset = top % 3

    if right_offset != 0:
        right_offset -= 3
    if top_offset != 0:
        top_offset -= 3

    left = left - left_offset
    bottom = bottom - bottom_offset
    right = right - right_offset
    top = top - top_offset
    return left, bottom, right, top


def add_odc_product(dc: Datacube):
    for _, doc in read_documents(PRODUCT):
        dc.index.products.add_document(doc)
    print("Product definition added")


def get_tile_uris(bounding_box: str) -> Tuple[str, str]:
    # Validate the bounding_box
    if bounding_box is None:
        logging.warning(
            "No BBOX provided, running full extent... this will take a long time."
        )
        bounding_box = (-180, -90, 180, 90)
    else:
        bounding_box = bounding_box.split(",")
        if len(bounding_box) != 4:
            raise ValueError("bounding_box must be in the format: minx,miny,maxx,maxy")
        bounding_box = [float(x) for x in bounding_box]

    # The tiles are 3 x 3 degree, starting at 0,3...
    # so we need some logic to get to those numbers
    left, bottom, right, top = _unpack_bbox(bounding_box)

    x_range = range(left, right, 3)
    y_range = range(bottom, top, 3)

    for x in x_range:
        for y in y_range:
            if x < 0:
                x_str = f"W{abs(x):03d}"
            else:
                x_str = f"E{x:03d}"
            if y < 0:
                y_str = f"S{abs(y):02d}"
            else:
                y_str = f"N{y:02d}"
            yield (
                URI_TEMPLATE.format(ns=y_str, ew=x_str),
                f"{x_str}_{y_str}",
            )


def process_uri_tile(
    uri_tile: Tuple[str, str, str],
    dc: Datacube,
    doc2ds: Doc2Dataset,
    update_if_exists: bool = True,
    archive_less_mature: bool = False,
    publish_action: str = None,
) -> Tuple[pystac.Item, str]:
    product_name = "esa_worldcover"
    uri, tile = uri_tile
    properties = {
        "odc:product": product_name,
        "odc:region_code": tile,
        "start_datetime": "2020-01-01",
        "end_datetime": "2020-12-31",
    }

    with rasterio.Env(aws_unsigned=True, GDAL_DISABLE_READDIR_ON_OPEN="EMPTY_DIR"):
        item = create_stac_item(
            uri,
            collection=product_name,
            with_proj=True,
            properties=properties,
            asset_media_type=pystac.MediaType.COG,
            asset_name="classification",
        )

    index_update_dataset(
        stac_transform(item.to_dict()),
        uri,
        dc,
        doc2ds,
        update_if_exists=update_if_exists,
        allow_unsafe=True,
        archive_less_mature=archive_less_mature,
        publish_action=publish_action,
        stac_doc=item.to_dict(),
    )

    return True


def esa_wc_to_dc(
    dc: Datacube,
    bounding_box,
    limit: int,
    update: bool,
    n_workers: int = 100,
    archive_less_mature: bool = False,
    publish_action: str = None,
) -> Tuple[int, int]:
    doc2ds = Doc2Dataset(dc.index)

    # Get a generator of (uris)
    uris_tiles = list(get_tile_uris(bounding_box))
    if limit:
        uris_tiles = uris_tiles[0:limit]

    # Do the indexing of all the things
    success = 0
    failure = 0

    sys.stdout.write(f"Starting ESA Worldcover indexing with {n_workers} workers...\n")

    with concurrent.futures.ThreadPoolExecutor(max_workers=n_workers) as executor:
        future_to_uri = {
            executor.submit(
                process_uri_tile,
                uri_tile,
                dc,
                doc2ds,
                update_if_exists=update,
                archive_less_mature=archive_less_mature,
                publish_action=publish_action,
            ): uri_tile[0]
            for uri_tile in uris_tiles
        }
        for future in concurrent.futures.as_completed(future_to_uri):
            uri = future_to_uri[future]
            try:
                _ = future.result()
                success += 1
                if success % 10 == 0:
                    sys.stdout.write(f"\rAdded {success} datasets...")
            except rasterio.errors.RasterioIOError:
                logging.info("Couldn't read file %s", uri, exc_info=True)
            except Exception:  # pylint:disable=broad-except
                logging.exception("Failed to handle uri %s", uri)
                failure += 1
    sys.stdout.write("\r")

    return success, failure


@click.command("esa-wc-to-dc")
@limit
@update_if_exists_flag
@bbox
@click.option(
    "--add-product",
    is_flag=True,
    default=False,
    help="If set, add the product too",
)
@archive_less_mature
@publish_action
@click.option(
    "--workers",
    default=20,
    type=int,
    help="Number of threads to use to process, default 20",
)
@statsd_setting
def cli(
    limit,
    update_if_exists,
    bbox,
    add_product,
    archive_less_mature,
    workers,
    statsd_setting,
    publish_action,
):
    """
    Index the ESA WorldCover product automatically.
    """

    dc = Datacube()

    if add_product:
        add_odc_product(dc)

    print(f"Indexing ESA WorldCover with bounding box of {bbox}")

    added, failed = esa_wc_to_dc(
        dc,
        bbox,
        limit,
        update_if_exists,
        n_workers=workers,
        archive_less_mature=archive_less_mature,
        publish_action=publish_action,
    )

    print(f"Added {added} Datasets, failed {failed} Datasets")

    if statsd_setting:
        statsd_gauge_reporting(
            added, ["app:esa_worldcover_to_dc", "action:added"], statsd_setting
        )
        statsd_gauge_reporting(
            failed, ["app:esa_worldcover_to_dc", "action:failed"], statsd_setting
        )

    if failed > 0:
        sys.exit(failed)


if __name__ == "__main__":
    cli()
