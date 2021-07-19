#!/usr/bin/env python3
"""
Index ESRI Land Cover automatically.
"""
import concurrent.futures
import datetime
import logging
import sys
from pathlib import Path
from typing import Dict, Tuple

import click
import pystac
import rasterio
from datacube import Datacube
from datacube.index.hl import Doc2Dataset
from datacube.utils import read_documents
from odc.apps.dc_tools.utils import (
    get_esri_list,
    index_update_dataset,
    limit,
    update_if_exists,
)
from odc.index.stac import stac_transform
from pyproj import Transformer

ESRI_LANDCOVER_PRODUCT = (
    "https://raw.githubusercontent.com/opendatacube/"
    "datacube-dataset-config/master/products/esri_land_cover.yaml"
)


def add_esri_lc_product(dc: Datacube):
    """ Add the ESRI Land Cover product definition"""
    for path, doc in read_documents(ESRI_LANDCOVER_PRODUCT):
        dc.index.products.add_document(doc)
    print("Product definition added.")


def bbox_to_geom(bbox: Tuple[float, float, float, float], crs=str) -> Dict:
    """Return a geojson geometry from a bbox."""

    transformer = Transformer.from_crs(crs, "EPSG:4326", always_xy=True)
    x_min, y_min = transformer.transform(bbox[0], bbox[1])
    x_max, y_max = transformer.transform(bbox[2], bbox[3])

    # Stupid dateline!
    if abs(x_max - x_min) > 90:
        if x_max < 0:
            x_max += 360
        if x_min < 0:
            x_min += 360

    return {
        "type": "Polygon",
        "coordinates": [
            [
                [x_min, y_min],
                [x_min, y_max],
                [x_max, y_max],
                [x_max, y_min],
                [x_min, y_min],
            ]
        ],
    }


def get_item(uri: str) -> Tuple[pystac.Item, str]:
    path = Path(uri)

    with rasterio.open(uri, GEOREF_SOURCES="INTERNAL") as opened_asset:
        shape = opened_asset.shape
        transform = opened_asset.transform
        crs = opened_asset.crs.to_epsg()
        bbox = opened_asset.bounds

    region, date_range = path.stem.split("_")
    start_date, end_date = date_range.split("-")
    item = pystac.Item(
        id=path.name,
        geometry=bbox_to_geom(bbox, crs),
        bbox=bbox,
        datetime=datetime.datetime.strptime(start_date, "%Y%m%d"),
        properties={
            "odc:product": "esri_land_cover",
            "odc:region_code": region.strip(),
        },
        stac_extensions=["projection"],
    )

    item.ext.projection.epsg = crs

    asset = pystac.Asset(
        href=uri,
        media_type=pystac.MediaType.COG,
        roles=["data"],
        title="classification",
    )
    item.add_asset("classification", asset)

    item.ext.projection.set_transform(transform, asset=asset)
    item.ext.projection.set_shape(shape, asset=asset)

    return item, uri


def esri_lc_to_dc(dc: Datacube, limit: int, update: bool) -> Tuple[int, int]:
    doc2ds = Doc2Dataset(dc.index)
    # Get a generator of (dataset_json, uri)
    docs = get_esri_list()
    if limit:
        docs = list(docs)[0:limit]

    with concurrent.futures.ThreadPoolExecutor(max_workers=20) as executor:
        items_uris = executor.map(get_item, docs)

    # Do the indexing of all the things
    success = 0
    failure = 0

    for item, uri in items_uris:
        dataset = stac_transform(item.to_dict(), relative=False)
        try:
            index_update_dataset(
                dataset, uri, dc, doc2ds, update_if_exists=update, allow_unsafe=True
            )
            success += 1
        except Exception as e:
            logging.warning(f"Failed to index {uri} with exception {e}")
            failure += 1

    return success, failure


@click.command("esri-lc-to-dc")
@limit
@update_if_exists
@click.option(
    "--add-product",
    is_flag=True,
    default=False,
    help="If set, add the product too",
)
def cli(limit, update_if_exists, add_product):
    """
    Add all of the ESRI Land Cover scenes to an ODC Database.
    Optionally add the product definition with `--add-product`.
    Default is to index all 700 scenes, but you can limit it for testing purposes with `--limit 1`.

    Add all the scenes in a new database with `esri-lc-to-dc --add-product`.
    """

    dc = Datacube()

    if add_product:
        add_esri_lc_product(dc)

    added, failed = esri_lc_to_dc(dc, limit, update_if_exists)

    print(f"Added {added} Datasets, failed {failed} Datasets")

    if failed > 0:
        sys.exit(failed)


if __name__ == "__main__":
    cli()
