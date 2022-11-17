"""
Tools for STAC to EO3 translation
"""
import math
from pathlib import Path
from typing import Any, Dict, Optional, Tuple
from uuid import UUID

import numpy
from datacube.utils.geometry import Geometry
from toolz import get_in
from urlpath import URL

from ._docs import odc_uuid

from eodatasets3.serialise import from_doc
from eodatasets3.stac import to_stac_item
from datacube.model import Dataset

Document = Dict[str, Any]

# This is an old hack, should be refactored out
DEA_LANDSAT_PRODUCTS = ["ga_ls8c_ard_3", "ga_ls7e_ard_3", "ga_ls8t_ard_3"]

# This is hack for not changing the current behavior of DEAfrica sentinel-2
# It is not ideal but to remain the impact minimum
TO_BE_HARD_CODED_COLLECTION = [
    "s2_l2a",
    "sentinel_s2_l2a_cogs",
    "sentinel-s2-l2a-cogs",
    "sentinel-2-l2a",
]

# Mapping between EO3 field names and STAC properties object field names
MAPPING_STAC_TO_EO3 = {
    "end_datetime": "dtr:end_datetime",
    "start_datetime": "dtr:start_datetime",
    "gsd": "eo:gsd",
    "instruments": "eo:instrument",
    "platform": "eo:platform",
    "constellation": "eo:constellation",
    "view:off_nadir": "eo:off_nadir",
    "view:azimuth": "eo:azimuth",
    "view:sun_azimuth": "eo:sun_azimuth",
    "view:sun_elevation": "eo:sun_elevation",
}

# Add more here as they are discovered
CANDIDATE_REGION_CODES = ["odc:region_code", "s2:mgrs_tile", "io:supercell_id"]


def _get_region_code(properties: Dict[str, Any]) -> str:
    # Check the default candidates
    for region_code_name in CANDIDATE_REGION_CODES:
        region_code = properties.get(region_code_name, None)
        if region_code is not None:
            return region_code

    # Landsat special case
    if region_code is None:
        row = get_in(["landsat:wrs_row"], properties, None)
        path = get_in(["landsat:wrs_path"], properties, None)
        try:
            if row is not None and path is not None:
                region_code = f"{int(path):03d}{int(row):03d}"
        except ValueError:
            pass

    return region_code


def _get_usgs_product_name(properties: Dict[str, Any]) -> str:
    platform = get_in(["platform"], properties)

    if platform == "LANDSAT_9":
        return "ls9-c2l2-sr"
    if platform == "LANDSAT_8":
        return "ls8-c2l2-sr"
    elif platform == "LANDSAT_7":
        return "ls7-c2l2-sr"
    elif platform == "LANDSAT_5":
        return "ls5-c2l2-sr"
    else:
        return None


def _stac_product_lookup(
    item: Document,
) -> Tuple[str, Optional[str], str, Optional[str], str]:
    properties = item["properties"]

    dataset_id: str = item["id"]
    dataset_label = item.get("title")

    # Get the simplest version of a product name
    product_name = properties.get("odc:product", None)

    # Try to get a known region_code
    region_code = _get_region_code(properties)

    default_grid = None

    # Maybe this should be the default product_name
    constellation = properties.get("constellation") or properties.get(
        "eo:constellation"
    )
    if constellation is not None:
        constellation = constellation.lower().replace(" ", "-")

    collection = item.get("collection")
    # Special case for USGS Landsat Collection 2
    if collection is not None and collection == "landsat-c2l2-sr":
        product_name = _get_usgs_product_name(properties)

    # It is an ugly hack without interrupting DEAfric sentinel 2
    if constellation is not None and product_name is None:
        if constellation == "sentinel-2" and collection in TO_BE_HARD_CODED_COLLECTION:
            dataset_id = properties.get("sentinel:product_id") or properties.get(
                "s2:granule_id", dataset_id
            )
            product_name = "s2_l2a"
            if region_code is None:
                # Let's try two options, and throw an exception if we still don't get it
                try:
                    # The 'mgrs' prefix (and STAC extension) started with STAC v1.0.0
                    region_code = (
                        f"{str(properties['proj:epsg'])[-2:]}"
                        f"{properties['mgrs:latitude_band']}"
                        f"{properties['mgrs:grid_square']}"
                    )
                except KeyError:
                    region_code = (
                        f"{str(properties['proj:epsg'])[-2:]}"
                        f"{properties['sentinel:latitude_band']}"
                        f"{properties['sentinel:grid_square']}"
                    )

            default_grid = "g10m"

    # If we still don't have a product name, use collection
    if product_name is None:
        product_name = collection
        if product_name is None:
            raise ValueError("Can't find product name from odc:product or collection.")

    # Product names can't have dashes in them
    product_name = product_name.replace("-", "_")

    if product_name in DEA_LANDSAT_PRODUCTS:
        self_href = _find_self_href(item)
        dataset_label = Path(self_href).stem.replace(".stac-item", "")
        default_grid = "g30m"

    # If the ID is not cold and numerical, assume it can serve a label.
    if (
        not dataset_label
        and not _check_valid_uuid(dataset_id)
        and not dataset_id.isnumeric()
    ):
        dataset_label = dataset_id

    return dataset_id, dataset_label, product_name, region_code, default_grid


def _find_self_href(item: Document) -> str:
    """
    Extracting product label from filename of the STAC document 'self' URL
    """
    self_uri = [
        link.get("href", "")
        for link in item.get("links", [])
        if link.get("rel") == "self"
    ]

    if len(self_uri) < 1:
        return None
    if len(self_uri) > 1:
        print("More than one links to 'self', returning the first one")
    return self_uri[0]


def _get_stac_bands(
    item: Document,
    default_grid: str,
    relative: bool = False,
    proj_shape: Optional[str] = None,
    proj_transform: Optional[str] = None,
) -> Tuple[Document, Document, Document]:
    # pylint: disable=too-many-locals
    bands = {}
    grids = {}
    accessories = {}

    self_link = _find_self_href(item)

    assets = item.get("assets", {})

    def _get_path(asset, force_relative=False):
        path = URL(asset["href"])
        if relative:
            try:
                if self_link is None:
                    raise ValueError
                path = path.relative_to(URL(self_link).parent)
            # Value error is raised if the path is not relative to the parent
            # or if the self link cannot be found.
            except ValueError:
                # If the path is not relative to the parent force_relative
                # is still used for data assets, due to a historical assumption.
                # TODO: Implement rewrite_assets (like in stac_to_dc) in all
                # tools so that this is no longer necessary.
                if force_relative:
                    path = path.name
                else:
                    pass

        return str(path)

    for asset_name, asset in assets.items():
        # If something's not a geotiff, make it an accessory
        # include thumbnails in accessories
        if "geotiff" not in asset.get("type", []) or "thumbnail" in asset.get(
            "roles", []
        ):
            accessories[asset_name] = {"path": _get_path(asset)}
            continue

        # If transform specified here in the asset it should override
        # the properties-specified transform.
        transform = asset.get("proj:transform") or proj_transform
        grid = f"g{transform[0]:g}m"

        # As per transform, shape here overrides properties
        shape = asset.get("proj:shape") or proj_shape

        if grid not in grids:
            grids[grid] = {
                "shape": shape,
                "transform": transform,
            }

        path = _get_path(asset, force_relative=True)
        band_index = asset.get("band", None)

        band_info = {"path": path}
        if band_index is not None:
            band_info["band"] = band_index

        # If we don't specify a default grid, label the first grid 'default'
        if not default_grid:
            default_grid = list(grids.keys())[0]

        if grid != default_grid:
            band_info["grid"] = grid

        bands[asset_name] = band_info

    if default_grid in grids:
        grids["default"] = grids.pop(default_grid)

    return bands, grids, accessories


def _geographic_to_projected(geometry, crs, precision=10):
    """Transform from WGS84 to the target projection, assuming Lon, Lat order"""
    geom = geometry.to_crs(crs, resolution=math.inf)

    def round_coords(c1, c2):
        return [round(coord, precision) for coord in [c1, c2]]

    if geom.is_valid:
        return geom.transform(round_coords)
    else:
        return None


def stac_transform_absolute(input_stac):
    return stac_transform(input_stac, relative=False)


def _convert_value_to_eo3_type(key: str, value):
    """
    Convert return type as per EO3 specification.
    Return type is String for "instrument" field in EO3 metadata.

    """
    if key == "instruments":
        if len(value) > 0:
            return "_".join([i.upper() for i in value])
        else:
            return None
    else:
        return value


def _get_stac_properties_lineage(input_stac: Document) -> Tuple[Document, Any]:
    """
    Extract properties and lineage field
    """
    properties = input_stac["properties"]
    prop = {
        MAPPING_STAC_TO_EO3.get(key, key): _convert_value_to_eo3_type(key, val)
        for key, val in properties.items()
    }

    creation_time = (
        properties.get("odc:processing_datetime")
        or properties.get("created")
        or properties.get("datetime")
    )
    if prop.get("odc:processing_datetime") is None and creation_time:
        prop["odc:processing_datetime"] = creation_time

    if prop.get("odc:file_format") is None:
        prop["odc:file_format"] = "GeoTIFF"

    # Extract lineage
    lineage = prop.pop("odc:lineage", None)

    return prop, lineage


def _check_valid_uuid(uuid_string: str) -> bool:
    """
    Check if provided uuid string is a valid UUID.
    """
    try:
        UUID(str(uuid_string))
        return True
    except ValueError:
        return False


def stac_transform(input_stac: Document, relative: bool = True) -> Document:
    """Takes in a raw STAC 1.0 dictionary and returns an ODC dictionary"""
    # pylint: disable=too-many-locals

    (
        dataset_id,
        dataset_label,
        product_name,
        region_code,
        default_grid,
    ) = _stac_product_lookup(input_stac)

    # Generating UUID for products not having UUID.
    # Checking if provided id is valid UUID.
    # If not valid, creating new deterministic uuid using odc_uuid function
    # based on product_name and product_label.
    # TODO: Verify if this approach to create UUID is valid.
    if _check_valid_uuid(input_stac["id"]):
        deterministic_uuid = input_stac["id"]
    else:
        if product_name in ["s2_l2a"]:
            deterministic_uuid = str(
                odc_uuid("sentinel-2_stac_process", "1.0.0", [dataset_id])
            )
        else:
            deterministic_uuid = str(
                odc_uuid(f"{product_name}_stac_process", "1.0.0", [dataset_id])
            )

    # Check for projection extension properties that are not in the asset fields.
    # Specifically, proj:shape and proj:transform, as these are otherwise
    # fetched in _get_stac_bands.
    properties = input_stac["properties"]
    proj_shape = properties.get("proj:shape")
    proj_transform = properties.get("proj:transform")
    # TODO: handle old STAC that doesn't have grid information here...
    bands, grids, accessories = _get_stac_bands(
        input_stac,
        default_grid,
        relative=relative,
        proj_shape=proj_shape,
        proj_transform=proj_transform,
    )

    stac_properties, lineage = _get_stac_properties_lineage(input_stac)

    epsg = properties["proj:epsg"]
    native_crs = f"epsg:{epsg}"

    # Transform geometry to the native CRS at an appropriate precision
    geometry = Geometry(input_stac["geometry"], "epsg:4326")
    if native_crs != "epsg:4326":
        # Arbitrary precisions, but should be fine
        pixel_size = get_in(["default", "transform", 0], grids, no_default=True)
        precision = 0
        if pixel_size < 0:
            precision = 6

        geometry = _geographic_to_projected(geometry, native_crs, precision)

    stac_odc = {
        "$schema": "https://schemas.opendatacube.org/dataset",
        "id": deterministic_uuid,
        "crs": native_crs,
        "grids": grids,
        "product": {"name": product_name.lower()},
        "properties": stac_properties,
        "measurements": bands,
        "lineage": {},
        "accessories": accessories,
    }
    if dataset_label:
        stac_odc["label"] = dataset_label

    if region_code:
        stac_odc["properties"]["odc:region_code"] = region_code

    if geometry:
        stac_odc["geometry"] = transform_geom_json_coordinates_to_list(geometry.json)

    if lineage:
        stac_odc["lineage"] = lineage

    return stac_odc


# TODO: This is a temporary fix
def transform_geom_json_coordinates_to_list(geom_json):
    geom_json["coordinates"] = numpy.array(geom_json["coordinates"]).tolist()
    return geom_json


def ds_to_stac(ds: Dataset) -> dict:
    """Get STAC document from dataset with eo3 metadata"""
    if ds.is_eo3:
        if not ds.uris:
            raise ValueError("Can't find dataset location")
        location = ds.uris[0]
        stac = to_stac_item(
            from_doc(ds.metadata_doc, skip_validation=True),
            location,
        )
        return stac
    else:
        raise ValueError(
            f"Cannot convert to STAC for dataset with metadata of type {ds.metadata_type}"
        )
