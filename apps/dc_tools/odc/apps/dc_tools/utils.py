import logging
from typing import Any, Dict, Optional, Tuple

import click
from datacube import Datacube
from datacube.index.hl import Doc2Dataset
from datacube.model import Dataset
from datacube.utils import changes, jsonify_document
from datadog import initialize, statsd
from odc.aws.queue import publish_to_topic
from datacube.metadata import stac2ds
from pystac import Item

from ._stac import ds_to_stac

MICROSOFT_PC_STAC_URI = "https://planetarycomputer.microsoft.com/api/stac/v1/"


class IndexingError(Exception):
    """
    Exception to raise for error during SQS to DC indexing/archiving
    """


class DatasetExists(Exception):
    """
    Exception to raise for error if dataset exists and not updating
    """


# A whole bunch of generic Click options
skip_lineage = click.option(
    "--skip-lineage",
    is_flag=True,
    default=False,
    help="Default is not to skip lineage. Set to skip lineage altogether.",
)

fail_on_missing_lineage = click.option(
    "--fail-on-missing-lineage",
    is_flag=True,
    help=(
        "Default is to permit unindexed/external lineage documents. "
        "Set flag to fail if lineage documents are not present in the database."
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
    is_flag=False,
    flag_value=500,
    default=None,
    type=int,
    help=(
        "Archive existing any datasets that match product, "
        "time and region-code, but have lower dataset-maturity."
        "Note: An error will be raised and the dataset add will "
        "fail if a matching dataset with higher or equal dataset-maturity."
        "Can specify an of leniency for comparing timestamps, provided in milliseconds. "
        "Default value is 500ms."
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

rename_product = click.option(
    "--rename-product",
    type=str,
    default=None,
    help=(
        "Name of product to overwrite collection(s) names, "
        "only one product name can overwrite, despite multiple collections "
    ),
)

url_string_replace = click.option(
    "--url-string-replace",
    type=str,
    default=None,
    help="Replace a string in the STAC API URLs, e.g., 'https://stac.example.com,s3://stac.example.org'",
)


def index_update_dataset(
    dataset: dict | Dataset,
    uri: str,
    dc: Datacube,
    doc2ds: Doc2Dataset | None,
    update: bool = False,
    update_if_exists: bool = False,
    allow_unsafe: bool = False,
    archive_less_mature: Optional[int] = None,
    auto_add_lineage: Optional[bool] = False,
    publish_action: Optional[str] = None,
    stac_doc: Optional[dict] = None,
):
    """
    Index and/or update a dataset.  Called by all the **_to_dc CLI tools.

    :param dataset: A dataset metadata dictionary, read from yaml or json, converted from STAC, etc.
    :param uri: The URI of the metadata and associated data.
    :param dc: A datacube object (carries a database index and potentially an active transaction).
    :param doc2ds: A Doc2Dataset object (metadata_type and product resolver)
    :param update: If true, allow update only.
    :param update_if_exists: If true allow insert or update.
    :param allow_unsafe: Allow unsafe (arbitrary) dataset updates.
    :param archive_less_mature: Enforce dataset maturity.
           * If None (the default), ignore dataset maturity.
           * If int, enforce dataset maturity by looking for existing datasets with same product, region_code and time
             values. If a less mature match is found, it is archived and replaced with the new dataset being inserted.
             If a match of the same or greater maturity is found a DatasetExists is raised.
             The integer value is used as the timedelta value for allowing a leniency when comparing
             timestamp values, for datasets where there is a slight discrepancy. Default is 500ms.
    :param publish_action: SNS topic arn to publish action to.
    :param stac_doc: STAC document for publication to SNS topic.
    :return: Returns nothing.  Raises an exception if anything goes wrong.
    """
    # Make sure we can create a dataset first
    if not isinstance(dataset, Dataset):
        try:
            if doc2ds is None:
                doc2ds = Doc2Dataset(dc.index)
            dataset, err = doc2ds(jsonify_document(dataset), uri)
        except ValueError as e:
            raise IndexingError(
                f"Exception thrown when trying to create dataset: '{e}'\n The URI was {uri}"
            ) from e
        if dataset is None:
            raise IndexingError(
                f"Failed to create dataset with error {err}\n The URI was {uri}"
            )

    with dc.index.transaction():
        # Process in a transaction
        archive_stacs = []
        added = False
        updated = False

        if isinstance(archive_less_mature, int) and publish_action:
            dupes = dc.index.datasets.find_less_mature(dataset, archive_less_mature)
            for dupe in dupes:
                archive_stacs.append(ds_to_stac(dupe))

        # Now do something with the dataset
        # Note that any of the exceptions raised below will rollback any archiving performed above.
        if dc.index.datasets.has(dataset.id):
            # Update
            if update or update_if_exists:
                # Set up update fields
                updates = {}
                if allow_unsafe:
                    updates = {tuple(): changes.allow_any}
                # Do the updating
                try:
                    dc.index.datasets.update(
                        dataset,
                        updates_allowed=updates,
                        archive_less_mature=archive_less_mature,
                    )
                    updated = True
                except ValueError as e:
                    raise IndexingError(
                        f"Updating the dataset raised an exception: {e}"
                    )
            else:
                raise DatasetExists(
                    f"Dataset {dataset.id} already exists, not indexing"
                )
        else:
            if update:
                # We're expecting to update a dataset, but it doesn't exist
                raise IndexingError("Can't update dataset because it doesn't exist.")
            # Everything is working as expected, add the dataset
            dc.index.datasets.add(
                dataset,
                with_lineage=auto_add_lineage,
                archive_less_mature=archive_less_mature,
            )
            added = True

    if publish_action:
        for arch_stac in archive_stacs:
            publish_to_topic(arn=publish_action, action="ARCHIVED", stac=arch_stac)

    if added:
        logging.info("New Dataset Added: %s", dataset.id)
        if publish_action:
            # if STAC was not provided, generate from dataset
            stac_doc = stac_doc if stac_doc else ds_to_stac(dataset)
            publish_to_topic(arn=publish_action, action="ADDED", stac=stac_doc)

    if updated:
        logging.info("Existing Dataset Updated: %s", dataset.id)


def statsd_gauge_reporting(value, tags=None, statsd_setting="localhost:8125") -> None:
    if tags is None:
        tags = []
    host = statsd_setting.split(":")[0]
    port = statsd_setting.split(":")[1]
    options = {"statsd_host": host, "statsd_port": port}
    initialize(**options)

    statsd.gauge("datacube_index", value, tags=tags)


def get_self_link(item: Item) -> str | None:
    uri = None
    for link in item.links:
        if link.rel == "self":
            uri = link.target

        # Override self with canonical
        if link.rel == "canonical":
            return link.target
    return uri


def item_to_meta_uri(
    item: Item,
    dc: Datacube,
    rename_product: Optional[str] = None,
    url_string_replace: tuple[str, str] | None = None,
) -> Tuple[Dataset, str | None, Dict[str, Any]]:
    if rename_product is not None:
        item.properties["odc:product"] = rename_product

    # If we need to modify URLs, do it for the main URL and the asset links
    uri = get_self_link(item)
    if url_string_replace is not None:
        old_url, new_url = url_string_replace

        if uri is not None:
            uri = uri.replace(old_url, new_url)

        for asset in item.assets.values():
            asset.href = asset.href.replace(old_url, new_url)

    # Try to the Datacube product for the dataset
    product_name = item.properties.get("odc:product", item.collection_id)
    product_name_sanitised = product_name.replace("-", "_")
    product = dc.index.products.get_by_name(product_name_sanitised)

    if product is None:
        logging.warning(
            "Couldn't find matching product for product name: %s",
            product_name_sanitised,
        )
        raise DatasetExists(
            f"Couldn't find matching product for product name: {product_name_sanitised}"
        )

    # Convert the STAC Item to a Dataset
    dataset = next(stac2ds([item], {"asset_absolute_paths": False}))
    # And assign the product ID
    dataset.product = product

    return (dataset, uri, item.to_dict(transform_hrefs=False))
