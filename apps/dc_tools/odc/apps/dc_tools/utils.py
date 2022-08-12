import os
import logging
import click

import pkg_resources
from datacube import Datacube
from datacube.index.hl import Doc2Dataset
from datacube.utils import changes

from datadog import statsd, initialize


ESRI_LANDCOVER_BASE_URI = (
    "https://ai4edataeuwest.blob.core.windows.net/io-lulc/"
    "io-lulc-model-001-v01-composite-v03-supercell-v02-clip-v01/{id}_20200101-20210101.tif"
)

MICROSOFT_PC_STAC_URI = "https://planetarycomputer.microsoft.com/api/stac/v1/"


class IndexingException(Exception):
    """
    Exception to raise for error during SQS to DC indexing/archiving
    """

    pass


# A whole bunch of generic Click options
skip_lineage = click.option(
    "--skip-lineage",
    is_flag=True,
    default=False,
    help="Default is not to skip lineage. Set to skip lineage altogether.",
)

fail_on_missing_lineage = click.option(
    "--fail-on-missing-lineage/--auto-add-lineage",
    is_flag=True,
    default=True,
    help=(
        "Default is to fail if lineage documents not present in the database. "
        "Set auto add to try to index lineage documents."
    ),
)

verify_lineage = click.option(
    "--verify-lineage",
    is_flag=True,
    default=False,
    help="Default is no verification. Set to verify parent dataset definitions.",
)

transform_stac = click.option(
    "--stac",
    is_flag=True,
    default=False,
    help="Expect STAC 1.0 metadata and attempt to transform to ODC EO3 metadata.",
)

transform_stac_absolute = click.option(
    "--absolute",
    is_flag=True,
    default=False,
    help="Use absolute paths from the STAC document.",
)

update = click.option(
    "--update",
    is_flag=True,
    default=False,
    help="If set, update instead of add datasets.",
)

update_if_exists = click.option(
    "--update-if-exists",
    is_flag=True,
    default=False,
    help="If the dataset or product already exists, update it instead of skipping it.",
)

allow_unsafe = click.option(
    "--allow-unsafe",
    is_flag=True,
    default=False,
    help="Allow unsafe changes to a dataset. Take care!",
)

skip_check = click.option(
    "--skip-check",
    is_flag=True,
    default=False,
    help="Assume file exists when listing exact file rather than wildcard.",
)

no_sign_request = click.option(
    "--no-sign-request", is_flag=True, default=False, help="Do not sign AWS S3 requests."
)

request_payer = click.option(
    "--request-payer",
    is_flag=True,
    default=False,
    help="Needed when accessing requester pays public buckets.",
)

archive = click.option(
    "--archive",
    is_flag=True,
    default=False,
    help="Archive datasets instead of adding them.",
)

limit = click.option(
    "--limit",
    default=None,
    type=int,
    help="Stop indexing after n datasets have been indexed.",
)

bbox = click.option(
    "--bbox",
    type=str,
    default=None,
    help="Comma separated list of bounding box coords, lon-min, lat-min, lon-max, lat-max",
)

statsd_setting = click.option(
    "--statsd-setting",
    is_flag=False,
    default=None,
    help="statsd exporter hostname and port, i.e. prometheus-statsd-exporter:9125"
)


def get_esri_list():
    stream = pkg_resources.resource_stream(__name__, "esri-lc-tiles-list.txt")
    with stream as f:
        for tile in f.readlines():
            id = tile.decode().rstrip('\n')
            yield ESRI_LANDCOVER_BASE_URI.format(id=id)


def index_update_dataset(
    metadata: dict,
    uri: str,
    dc: Datacube,
    doc2ds: Doc2Dataset,
    update=False,
    update_if_exists=False,
    allow_unsafe=False,
):
    if uri is not None:
        # Make sure we can create a dataset first
        try:
            ds, err = doc2ds(metadata, uri)
        except ValueError as e:
            raise IndexingException(
                f"Exception thrown when trying to create dataset: '{e}'\n The URI was {uri}"
            )

        # Now do something with the dataset
        if ds is not None:
            if dc.index.datasets.has(metadata.get("id")):
                # Update
                if update or update_if_exists:
                    # Set up update fields
                    updates = {}
                    if allow_unsafe:
                        updates = {tuple(): changes.allow_any}
                    # Do the updating
                    try:
                        dc.index.datasets.update(ds, updates_allowed=updates)
                    except ValueError as e:
                        raise IndexingException(
                            f"Updating the dataset raised an exception: {e}"
                        )
                else:
                    logging.warning("Dataset already exists, not indexing")
            else:
                if update:
                    # We're expecting to update a dataset, but it doesn't exist
                    raise IndexingException(
                        "Can't update dataset because it doesn't exist."
                    )
                # Everything is working as expected, add the dataset
                dc.index.datasets.add(ds)
        else:
            raise IndexingException(
                f"Failed to create dataset with error {err}\n The URI was {uri}"
            )
    else:
        raise IndexingException("Failed to get URI from metadata doc")


def statsd_gauge_reporting(
    metrics, value, tags=[],
    statsd_setting="localhost:8125"
):
    host = statsd_setting.split(":")[0]
    port = statsd_setting.split(":")[1]
    options = {
        'statsd_host': host,
        'statsd_port': port
    }
    initialize(**options)

    if os.environ.get("HOSTNAME"):
        tags.append(f"pod:{os.getenv('HOSTNAME')}")
    statsd.gauge(metrics, value, tags=tags)