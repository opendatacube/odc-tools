import logging
import os
from typing import Iterable, Optional, Union

import click
import pkg_resources
from datacube import Datacube
from datacube.index.hl import Doc2Dataset
from datacube.utils import changes

from datadog import statsd, initialize

from odc.aws.queue import publish_to_topic
from ._stac import ds_to_stac

ESRI_LANDCOVER_BASE_URI = (
    "https://ai4edataeuwest.blob.core.windows.net/io-lulc/"
    "io-lulc-model-001-v01-composite-v03-supercell-v02-clip-v01/{id}_20200101-20210101.tif"
)

MICROSOFT_PC_STAC_URI = "https://planetarycomputer.microsoft.com/api/stac/v1/"


class IndexingException(Exception):
    """
    Exception to raise for error during SQS to DC indexing/archiving
    """


class SkippedException(Exception):
    """
    Exception to raise for error if dataset exists  and not updating
    """


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

update_flag = click.option(
    "--update",
    is_flag=True,
    default=False,
    help="If set, update instead of add datasets.",
)

update_if_exists_flag = click.option(
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
    "--no-sign-request",
    is_flag=True,
    default=False,
    help="Do not sign AWS S3 requests.",
)

request_payer = click.option(
    "--request-payer",
    is_flag=True,
    default=False,
    help="Needed when accessing requester pays public buckets.",
)

archive_less_mature = click.option(
    "--archive-less-mature",
    is_flag=True,
    default=False,
    help=(
        "Archive existing any datasets that match product, "
        "time and region-code, but have lower dataset-maturity."
        "Note: An error will be raised and the dataset add will "
        "fail if a matching dataset with higher or equal dataset-maturity."
    ),
)

publish_action = click.option(
    "--publish-action",
    type=str,
    default=None,
    nargs=1,
    help="SNS topic arn to publish indexing/archiving actions to.",
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
    help="statsd exporter hostname and port, i.e. prometheus-statsd-exporter:9125",
)


def get_esri_list():
    stream = pkg_resources.resource_stream(__name__, "esri-lc-tiles-list.txt")
    with stream as f:
        for tile in f.readlines():
            tile_id = tile.decode().rstrip("\n")
            yield ESRI_LANDCOVER_BASE_URI.format(id=tile_id)


def index_update_dataset(
    metadata: dict,
    uri: str,
    dc: Datacube,
    doc2ds: Doc2Dataset,
    update: bool = False,
    update_if_exists: bool = False,
    allow_unsafe: bool = False,
    archive_less_mature: Optional[Union[bool, Iterable[str]]] = None,
    publish_action: Optional[str] = None,
    stac_doc: Optional[dict] = None,
) -> int:
    """
    Index and/or update a dataset.  Called by all the **_to_dc CLI tools.

    :param metadata: A dataset metadata dictionary, read from yaml or json, converted from STAC, etc.
    :param uri: The URI of the metadata and associated data.
    :param dc: A datacube object (carries a database index and potentially an active transaction).
    :param doc2ds: A Doc2Dataset object (metadata_type and product resolver)
    :param update: If true, allow update only.
    :param update_if_exists: If true allow insert or update.
    :param allow_unsafe: Allow unsafe (arbitrary) dataset updates.
    :param archive_less_mature: Enforce dataset maturity.
           * If None (the default) or False or an empty iterable, ignore dataset maturity.
           * If True, enforce dataset maturity by looking for existing datasets with same product, region_code and time
             values. If a less mature match is found, it is archived and replaced with the new dataset being inserted.
             If a match of the same or greater maturity is found an IndexException is raised.
           * If an iterable of valid search field names is provided, it is used as the "grouping" fields for
             identifying dataset maturity matches.
             (i.e. `archive_less_mature=True` is the same as `archive_less_mature=['region_code', 'time'])
    :param publish_action: SNS topic arn to publish action to.
    :param stac_doc: STAC document for publication to SNS topic.
    :return: Returns nothing.  Raises an exception if anything goes wrong.
    """
    if uri is None:
        raise IndexingException("Failed to get URI from metadata doc")
    # Make sure we can create a dataset first
    try:
        ds, err = doc2ds(metadata, uri)
    except ValueError as e:
        raise IndexingException(
            f"Exception thrown when trying to create dataset: '{e}'\n The URI was {uri}"
        ) from e
    if ds is None:
        raise IndexingException(
            f"Failed to create dataset with error {err}\n The URI was {uri}"
        )

    if archive_less_mature:
        if archive_less_mature is True:
            # if set explicitly to True, default to [region_code, time]
            archive_less_mature = ["region_code", "time"]
        try:
            dupe_query = {k: getattr(ds.metadata, k) for k in archive_less_mature}
        except AttributeError as e:
            raise IndexingException(
                f"Cannot extract matching value from dataset for maturity check: {e}\n The URI was {uri}"
            )
    else:
        dupe_query = {}

    with dc.index.transaction():
        # Process in a transaction
        archive_ids = []
        archive_stacs = []
        added = False
        updated = False
        if archive_less_mature:
            dupes = dc.index.datasets.search(product=ds.type.name, **dupe_query)
            for dupe in dupes:
                if dupe.id == ds.id:
                    # Same dataset, for update.  Ignore
                    continue
                if dupe.metadata.dataset_maturity <= ds.metadata.dataset_maturity:
                    # Duplicate is as mature, or more mature than ds
                    # E.g. "final" < "nrt"
                    raise IndexingException(
                        f"Matching dataset of maturity {dupe.metadata.dataset_maturity} already exists"
                        "with id: {dupe.id}\n"
                        f"Cannot load dataset of maturity {ds.metadata.dataset_maturity} URI {uri} "
                    )
                archive_ids.append(dupe.id)
                if publish_action:
                    archive_stacs.append(ds_to_stac(dupe))
            if archive_ids:
                dc.index.datasets.archive(archive_ids)

        # Now do something with the dataset
        # Note that any of the exceptions raised below will rollback any archiving performed above.
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
                    updated = True
                except ValueError as e:
                    raise IndexingException(
                        f"Updating the dataset raised an exception: {e}"
                    )
            else:
                logging.warning("Dataset already exists, not indexing")
                raise SkippedException(
                    f"Dataset {metadata.get('id')} already exists, not indexing"
                )
        else:
            if update:
                # We're expecting to update a dataset, but it doesn't exist
                raise IndexingException(
                    "Can't update dataset because it doesn't exist."
                )
            # Everything is working as expected, add the dataset
            dc.index.datasets.add(ds)
            added = True

    # Transaction committed : Log actions
    for arch_id in archive_ids:
        logging.info("Archived less mature dataset: %s", arch_id)
    if publish_action:
        for arch_stac in archive_stacs:
            publish_to_topic(arn=publish_action, action="ARCHIVED", stac=arch_stac)

    if added:
        logging.info("New Dataset Added: %s", ds.id)
        if publish_action:
            # if STAC was not provided, generate from dataset
            stac_doc = stac_doc if stac_doc else ds_to_stac(ds)
            publish_to_topic(arn=publish_action, action="ADDED", stac=stac_doc)

    if updated:
        logging.info("Existing Dataset Updated: %s", ds.id)


def statsd_gauge_reporting(value, tags=None, statsd_setting="localhost:8125"):
    if tags is None:
        tags = []
    host = statsd_setting.split(":")[0]
    port = statsd_setting.split(":")[1]
    options = {"statsd_host": host, "statsd_port": port}
    initialize(**options)

    if os.environ.get("HOSTNAME"):
        tags.append(f"pod:{os.getenv('HOSTNAME')}")
    statsd.gauge("datacube_index", value, tags=tags)
