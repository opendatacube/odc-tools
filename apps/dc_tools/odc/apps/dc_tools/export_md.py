"""
Metadata Transformation from old format to new format:
Example usage:
 python export_md.py --datacube-config $HOME/.datacube.conf transform --product aster_l1t_swir
    --config metadata_transform_config.yaml --output-dir /g/data/u46/users/aj9439/metadata
    --limit 10
The config file contains the grid mappings to band names:
    grids:
      default: ['3', '4', '5']
      swir: ['1', '2']
"""
import logging
from pathlib import Path

import click
import yaml
from datacube import Datacube
from datacube.storage import BandInfo
from datacube.testutils.io import native_geobox

_LOG = logging.getLogger(__name__)


@click.group(help=__doc__)
@click.option(
    "--datacube-config",
    "-c",
    help="Pass the configuration file to access the database",
    type=click.Path(exists=True),
)
@click.pass_context
def cli(ctx, datacube_config):
    """Specify datacube index to be used for the given datacube config"""
    ctx.obj = Datacube(config=datacube_config).index


@cli.command()
@click.option("--product", required=True, help="Which product?")
@click.option(
    "--config", required=True, help="The configuration file for transformation"
)
@click.option(
    "--output-dir", required=True, help="New metadata yaml file is written to this dir"
)
@click.option("--limit", help="maximum number of datasets to process")
@click.pass_obj
def transform(index, product, config, output_dir, limit):

    # Get the product
    dataset_type = index.products.get_by_name(product)

    with open(config, "r") as config_file:
        cfg = yaml.load(config_file) or dict()

    # Is this a ingested product?
    if dataset_type.grid_spec is not None:
        transform_ingested_datasets(index, product, cfg, Path(output_dir), limit)
    else:
        transform_indexed_datasets(index, product, cfg, Path(output_dir), limit)


def transform_ingested_datasets(index, product, config, output_dir, limit):
    """
    Transform the metadata of ingested product. The product-wide fixed sections
    of metadata such as 'grids' is computed just once.
    """

    dataset_ids = index.datasets.search_returning(
        limit=limit, field_names=("id",), product=product
    )

    grids_done = False
    for dataset_id in dataset_ids:

        dataset = index.datasets.get(dataset_id.id, include_sources=True)

        if not dataset.uris:
            _LOG.warn("Empty uris or No uris (skippins): %s", dataset_id)
            continue

        if not grids_done:
            # setup grids
            grids = get_grids(dataset, config.get("grids"))

            # got to try to compute grids until shape is not default [1, 1]
            grids_done = all(
                [[1, 1] != grid["shape"] for _, grid in grids["grids"].items()]
            )

        dataset_sections = (grids,) + _variable_sections_of_metadata(dataset, config)
        _make_and_write_dataset(get_output_file(dataset, output_dir), *dataset_sections)


def transform_indexed_datasets(index, product, config, output_dir, limit):
    """
    Transform metadata of an indexed product. All sections of metadata are computed
    per dataset.
    """

    for dataset_id in index.datasets.search_returning(
        limit=limit, field_names=("id",), product=product
    ):

        dataset = index.datasets.get(dataset_id.id, include_sources=True)

        if not dataset.uris:
            _LOG.warn("Empty or No uris (skipping): %s", dataset_id)
            continue

        grids = get_grids(dataset, config.get("grids"))

        dataset_sections = (grids,) + _variable_sections_of_metadata(dataset, config)
        _make_and_write_dataset(get_output_file(dataset, output_dir), *dataset_sections)


def get_output_file(dataset, output_dir):
    """
    Compute the output metadata file name for the given dataset.
    """

    out_file_name = str(dataset.id) + "-metadata.yaml"

    return output_dir / out_file_name


def _variable_sections_of_metadata(dataset, config):
    """
    Compute variable sections (i.e. those sections that vary per dataset)
    """
    new_dataset = {
        "id": str(dataset.id),
        "crs": "EPSG:" + str(dataset.crs.epsg),
        "location": [uri for uri in dataset.uris],
        "file_format": dataset.metadata_doc.get("format", ""),
    }

    return (
        new_dataset,
        get_geometry(dataset),
        get_measurements(dataset, config.get("grids")),
        get_properties(dataset),
        get_lineage(dataset),
    )


def _make_and_write_dataset(out_file_name, *args):
    """
    Assemble the metadata sections and write out.
    """

    dataset = dict()
    for arg in args:
        dataset.update(arg)

    with open(out_file_name, "w") as out_file:
        yaml.dump(dataset, out_file, default_flow_style=False)


def get_geometry(dataset):
    """
    Extract and return geometry coordinates as a list in a dictionary:
    returns
    {
      'geometry': {
                    'type' : 'Polygon',
                    'coordinates': [...]
                  }
    }
    or
    empty dictionary
    """

    valid_data = dataset._gs.get("valid_data")

    return {"geometry": valid_data} if valid_data else dict()


def get_grids(dataset, band_grids=None):
    """
    'band_grids' specify bands for each grid as in:
    {
        default: [swir1, swir2]
        ir: [ir1, ir2]
    }
    All the bands not included in the 'band_grids' considered 'default'
    Returns grids as in
    {
     grids:
        default:
            shape: [7731, 7621]
            transform: [30.0, 0.0, 306285.0, 0.0, -30.0, -1802085.0, 0, 0, 1]
        ir:
            shape: [3865, 3810]
            transform: [60.0, 0.0, 306285.0, 0.0, -60.0, -1802085.0, 0, 0, 1]
    }
    """

    if not band_grids:
        # Assume all measurements belong to default grid
        shape, trans = get_shape_and_transform(dataset, dataset.measurements)

        return {"grids": {"default": {"shape": list(shape), "transform": list(trans)}}}
    else:
        grids = dict()
        for grid_name in band_grids:

            shape, trans = get_shape_and_transform(dataset, band_grids[grid_name])

            grids[grid_name] = {"shape": list(shape), "transform": list(trans)}

        if not band_grids.get("default"):

            specified_bands = set()
            for grid in band_grids:
                specified_bands.update(band_grids[grid])
            all_bands = set(list(dataset.measurements))

            default_bands = all_bands - specified_bands

            if bool(default_bands):
                shape, trans = get_shape_and_transform(dataset, default_bands)

                grids["default"] = {"shape": list(shape), "transform": list(trans)}
        return {"grids": grids}


def get_shape_and_transform(dataset, measurements):
    """
    Get shape and transform for the given set of measurements. It try
    each measurement until it succeeds.
    """

    for m in measurements:
        try:
            geo = native_geobox(dataset, [m])
        except Exception:
            _LOG.warn("Failed to compute shape and transform %s", m)
            continue
        return geo.shape, geo.transform

    # All the bands failed use default shape [1,1]
    _LOG.warn("All measurements failed to compute shape and transform %s", measurements)
    return [1, 1], dataset.transform


def get_measurements(dataset, band_grids=None):
    """
    Extract and return measurement paths in a dictionary:
    Returns
    {
      'measurements':
      {
        'coastal_aerosol': {
                             'path': path_name1,
                             'band': band_number,
                             'layer': null or 'layer_name'
                             'grid': 'ir'
                           },
        ...
      }
    }
    """
    grids_map = (
        {m: grid for grid in band_grids for m in band_grids[grid] if grid != "default"}
        if band_grids
        else dict()
    )

    measurements = dict()
    for m in dataset.measurements:

        if m not in dataset.type.measurements:
            _LOG.warn("Measurement not in product definition (skipping): %s", m)
            continue

        band_info = BandInfo(dataset, m)
        measurements[m] = {"path": dataset.measurements[m]["path"]}

        if band_info.band and band_info.band != 1:
            measurements[m]["band"] = band_info.band

        if band_info.layer is not None:
            measurements[m]["layer"] = band_info.layer

        if grids_map.get(m):
            measurements[m]["grid"] = grids_map[m]

    return {"measurements": measurements}


def get_properties(dataset, property_offsets=None):
    """
    Extract properties and return values in a dictionary:
    {
      'properties':
      {
        'datetime': time,
        'odc:creation_datetime': creation_time,
        ...
      }
    }
    """
    props = dict()
    props["datetime"] = dataset.center_time
    props["odc:processing_datetime"] = dataset.indexed_time

    return {"properties": props}


def get_lineage(dataset):
    """
    Extract immediate parents.
    {
      'lineage':
      {
        'nbar': [id1, id2],
        'pq': [id3]
        ...
      }
    }
    """
    lineage = dict()
    for classifier in dataset.sources:
        lineage[classifier] = str(dataset.sources[classifier].id)
    return {"lineage": lineage}


if __name__ == "__main__":
    cli()
