#!/usr/bin/env python3
"""
Index the Copernicus DEM automatically.
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
    SkippedException,
    archive_less_mature,
    bbox,
    index_update_dataset,
    limit,
    update_if_exists_flag,
    publish_action,
    statsd_gauge_reporting,
    statsd_setting,
)
from rio_stac import create_stac_item

from ._stac import stac_transform

PRODUCTS = {
    "cop_30": (
        "https://raw.githubusercontent.com/opendatacube/"
        "datacube-dataset-config/master/products/dem_cop_30.odc-product.yaml"
    ),
    "cop_90": (
        "https://raw.githubusercontent.com/opendatacube/"
        "datacube-dataset-config/master/products/dem_cop_90.odc-product.yaml"
    ),
}

# URIs need north/south, which is N00 and east/west, which is E000
URI_TEMPLATES = {
    "cop_30": (
        "https://copernicus-dem-30m.s3.eu-central-1.amazonaws.com/"
        "Copernicus_DSM_COG_10_{ns}_00_{ew}_00_DEM/Copernicus_DSM_COG_10_{ns}_00_{ew}_00_DEM.tif"
    ),
    "cop_90": (
        "https://copernicus-dem-90m.s3.eu-central-1.amazonaws.com/"
        "Copernicus_DSM_COG_30_{ns}_00_{ew}_00_DEM/Copernicus_DSM_COG_30_{ns}_00_{ew}_00_DEM.tif"
    ),
}


def add_cop_dem_product(dc: Datacube, product):
    if product in PRODUCTS:
        product_uri = PRODUCTS[product]
    else:
        raise ValueError(f"Unknown product {product}")

    for _, doc in read_documents(product_uri):
        dc.index.products.add_document(doc)
    print(f"Product definition added for {product}")


def get_dem_tile_uris(bounding_box, product):
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

    # Get the uris
    left = bounding_box[0]
    right = bounding_box[2]
    bottom = bounding_box[1]
    top = bounding_box[3]

    x_range = range(floor(left), ceil(right))
    y_range = range(floor(bottom), ceil(top))

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
                URI_TEMPLATES[product].format(ns=y_str, ew=x_str),
                f"{x_str}_{y_str}",
            )


def process_uri_tile(
    uri_tile: Tuple[str, str, str],
    product: str,
    dc: Datacube,
    doc2ds: Doc2Dataset,
    update_if_exists: bool = True,
    archive_less_mature: bool = False,
    publish_action: str = None,
) -> Tuple[pystac.Item, str]:
    product_name = f"dem_{product}"
    uri, tile = uri_tile
    properties = {
        "odc:product": product_name,
        "odc:region_code": tile,
        "start_datetime": "1900-01-01",
        "end_datetime": "2100-01-01",
    }

    with rasterio.Env(aws_unsigned=True, GDAL_DISABLE_READDIR_ON_OPEN="EMPTY_DIR"):
        item = create_stac_item(
            uri,
            collection=product_name,
            with_proj=True,
            properties=properties,
            asset_media_type=pystac.MediaType.COG,
            asset_name="elevation",
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


def cop_dem_to_dc(
    dc: Datacube,
    product: str,
    bounding_box,
    limit: int,
    update: bool,
    n_workers: int = 100,
    archive_less_mature=None,
    publish_action=None,
) -> Tuple[int, int]:
    doc2ds = Doc2Dataset(dc.index)

    # Get a generator of (uris)
    uris_tiles = list(get_dem_tile_uris(bounding_box, product))
    if limit:
        uris_tiles = uris_tiles[0:limit]

    # Do the indexing of all the things
    success = 0
    failure = 0
    skipped = 0

    sys.stdout.write(f"Starting Cop DEM indexing with {n_workers} workers...\n")

    with concurrent.futures.ThreadPoolExecutor(max_workers=n_workers) as executor:
        future_to_uri = {
            executor.submit(
                process_uri_tile,
                uri_tile,
                product,
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
            except SkippedException:
                skipped += 1
            except rasterio.errors.RasterioIOError:
                logging.info("Couldn't read file %s", uri, exc_info=True)
            except Exception:  # pylint:disable=broad-except
                logging.exception("Failed to handle uri %s", uri)
                failure += 1
    sys.stdout.write("\r")

    return success, failure, skipped


@click.command("cop-dem-to-dc")
@limit
@update_if_exists_flag
@bbox
@statsd_setting
@archive_less_mature
@publish_action
@click.option(
    "--product",
    default="cop_30",
    help="Product to add to the index, either cop_30 or cop_90",
)
@click.option(
    "--add-product",
    is_flag=True,
    default=False,
    help="If set, add the product too",
)
@click.option(
    "--workers",
    default=20,
    type=int,
    help="Number of threads to use to process, default 20",
)
def cli(
    limit,
    update_if_exists,
    bbox,
    statsd_setting,
    product,
    add_product,
    workers,
    archive_less_mature,
    publish_action,
):
    """
    Index the Copernicus DEM automatically.
    """
    if product not in PRODUCTS:
        raise ValueError(
            f"Unknown product {product}, must be one of {' '.join(PRODUCTS)}"
        )

    dc = Datacube()

    if add_product:
        add_cop_dem_product(dc, product)

    print(f"Indexing Copernicus DEM for {product} with bounding box of {bbox}")

    added, failed, skipped = cop_dem_to_dc(
        dc,
        product,
        bbox,
        limit,
        update_if_exists,
        n_workers=workers,
        archive_less_mature=archive_less_mature,
        publish_action=publish_action,
    )

    print(
        f"Added {added} Datasets, failed {failed} Datasets, skipped {skipped} Datasets"
    )

    if statsd_setting:
        statsd_gauge_reporting(
            added, ["app:cop_dem_to_dc", "action:added"], statsd_setting
        )
        statsd_gauge_reporting(
            failed, ["app:cop_dem_to_dc", "action:failed"], statsd_setting
        )
        statsd_gauge_reporting(
            skipped, ["app:cop_dem_to_dc", "action:skipped"], statsd_setting
        )

    if failed > 0:
        sys.exit(failed)


if __name__ == "__main__":
    cli()
