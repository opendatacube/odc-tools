#!/usr/bin/env python3
"""Index datasets found from an SQS queue into Postgres
"""
import json
import logging
import sys
import uuid
from pathlib import PurePath
from typing import Tuple

import boto3
import click
import pandas as pd
import requests
from datacube import Datacube
from datacube.index.hl import Doc2Dataset
from datacube.utils import changes, documents
from odc.aws.queue import get_messages
from odc.index.stac import stac_transform
from toolz import dicttoolz
from yaml import load

# Added log handler
logging.basicConfig(level=logging.INFO, handlers=[logging.StreamHandler()])


class SQStoDCException(Exception):
    """
    Exception to raise for error during SQS to DC indexing/archiving
    """

    pass


def extract_metadata_from_message(message):
    try:
        body = json.loads(message.body)
        metadata = json.loads(body["Message"])
    except (KeyError, json.JSONDecodeError) as e:
        raise SQStoDCException(
            f"Failed to load metadata from the SQS message due to error: {e}"
        )

    if metadata:
        return metadata
    else:
        raise SQStoDCException(f"Failed to load metadata from the SQS message")


def handle_json_message(metadata, transform, odc_metadata_link):
    odc_yaml_uri = None
    uri = None

    if odc_metadata_link:
        if odc_metadata_link.startswith("STAC-LINKS-REL:"):
            rel_val = odc_metadata_link.replace("STAC-LINKS-REL:", "")
            odc_yaml_uri = get_uri(metadata, rel_val)
        else:
            # if odc_metadata_link is provided, it will look for value with dict path provided
            odc_yaml_uri = dicttoolz.get_in(odc_metadata_link.split("/"), metadata)

        # if odc_yaml_uri exist, it will load the metadata content from that URL
        if odc_yaml_uri:
            try:
                content = requests.get(odc_yaml_uri).content
                metadata = documents.parse_yaml(content)
                uri = odc_yaml_uri
            except requests.RequestException as err:
                raise SQStoDCException(
                    f"Failed to load metadata from the link provided -  {err}"
                )
        else:
            raise SQStoDCException("ODC EO3 metadata link not found")
    else:
        # if no odc_metadata_link provided, it will look for metadata dict "href" value with "rel==self"
        uri = get_uri(metadata, "self")

    if transform:
        try:
            metadata = transform(metadata)
        except KeyError as err:
            raise SQStoDCException(
                f"Failed to transform metadata from {uri} with error - {err}"
            )

    return metadata, uri


def handle_bucket_notification_message(
    message, metadata: dict, record_path: tuple
) -> Tuple[dict, str]:
    """[summary]

    Args:
        message (Message resource)
        metadata (dict): [description]
        record_path (tuple): [PATH for selectingthe s3 key path from the JSON message document]

    Raises:
        SQStoDCException: [Catch s3 ]

    Returns:
        Tuple[dict, str]: [description]
    """
    data = None
    uri = None

    if metadata.get("Records"):
        for record in metadata.get("Records"):
            bucket_name = dicttoolz.get_in(["s3", "bucket", "name"], record)
            key = dicttoolz.get_in(["s3", "object", "key"], record)

            # Check for bucket name and key, and fail if there isn't one
            if not (bucket_name and key):
                # Not deleting this message, as it's non-conforming. Check this logic
                raise SQStoDCException(
                    "No bucket name or key in message, are you sure this is a bucket notification?"
                )

            # If you specific a list of record paths, and there's no
            # match in them for the key, then we skip this one forever
            if record_path is not None and not any(
                [PurePath(key).match(p) for p in record_path]
            ):
                message.delete()
                logging.warning(
                    f"Key: {key} not in specified list of record_paths, deleting message from the queue."
                )

            # We have enough information to proceed, get the key and extract
            # the contents...
            try:
                s3 = boto3.resource("s3")
                obj = s3.Object(bucket_name, key).get(ResponseCacheControl="no-cache")
                data = load(obj["Body"].read())
                uri = f"s3://{bucket_name}/{key}"
            except Exception as e:
                raise SQStoDCException(
                    f"Exception thrown when trying to load s3 object: '{e}'\n"
                )
    else:
        raise SQStoDCException(
            f"Attempted to get metadata from record when no record key exists in message."
        )

    return data, uri


def get_uri(metadata, rel_value):
    uri = None
    for link in metadata.get("links"):
        rel = link.get("rel")
        if rel and rel == rel_value:
            uri = link.get("href")
    return uri


def do_archiving(metadata, dc: Datacube):
    dataset_id = uuid.UUID(metadata.get("id"))
    if dataset_id:
        dc.index.datasets.archive([dataset_id])
    else:
        raise SQStoDCException(
            f"Archive skipped as failed to dataset with ID: {dataset_id}"
        )


def do_index_update_dataset(
    metadata: dict,
    uri,
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
            raise SQStoDCException(
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
                    dc.index.datasets.update(ds, updates_allowed=updates)
                else:
                    logging.warning("Dataset already exists, not indexing")
            else:
                if update:
                    # We're expecting to update a dataset, but it doesn't exist
                    raise SQStoDCException("Can't update dataset because it doesn't exist.")
                # Everything is working as expected, add the dataset
                dc.index.datasets.add(ds)
        else:
            raise SQStoDCException(
                f"Failed to create dataset with error {err}\n The URI was {uri}"
            )
    else:
        raise SQStoDCException("Failed to get URI from metadata doc")


def queue_to_odc(
    queue,
    dc: Datacube,
    products: list,
    record_path=None,
    transform=None,
    limit=None,
    update=False,
    update_if_exists=False,
    archive=False,
    allow_unsafe=False,
    odc_metadata_link=False,
    region_code_list_uri=None,
    **kwargs,
) -> Tuple[int, int]:

    ds_success = 0
    ds_failed = 0

    region_codes = None
    if region_code_list_uri:
        try:
            region_codes = set(pd.read_csv(region_code_list_uri).values.ravel())
        except FileNotFoundError as e:
            logging.error(f"Could not find region_code file with error: {e}")
        if len(region_codes) == 0:
            raise SQStoDCException(
                f"Region code list is empty, please check the list at: {region_code_list_uri}"
            )

    doc2ds = Doc2Dataset(dc.index, products=products, **kwargs)

    # This is a generator of messages
    messages = get_messages(queue, limit)

    for message in messages:
        try:
            # Extract metadata from message
            metadata = extract_metadata_from_message(message)
            if archive:
                # Archive metadata
                do_archiving(metadata, dc)
            else:
                if not record_path:
                    # Extract metadata and URI from a STAC or similar
                    # json structure for indexing
                    metadata, uri = handle_json_message(
                        metadata, transform, odc_metadata_link
                    )
                else:
                    # Extract metadata from an S3 bucket notification
                    # or similar for indexing
                    metadata, uri = handle_bucket_notification_message(
                        message, metadata, record_path
                    )

                # If we have a region_code filter, do it here
                if region_code_list_uri:
                    region_code = dicttoolz.get_in(
                        ["properties", "odc:region_code"], metadata
                    )
                    if region_code not in region_codes:
                        # We  don't want to keep this one, so delete the message
                        message.delete()
                        # And fail it...
                        raise SQStoDCException(
                            f"Region code {region_code} not in list of allowed region codes, ignoring this dataset."
                        )

            # Index the dataset
            do_index_update_dataset(metadata, uri, dc, doc2ds, update, update_if_exists, allow_unsafe)
            ds_success += 1
            # Success, so delete the message.
            message.delete()
        except SQStoDCException as err:
            logging.error(err)
            ds_failed += 1

    return ds_success, ds_failed


@click.command("sqs-to-dc")
@click.option(
    "--skip-lineage",
    is_flag=True,
    default=False,
    help="Default is not to skip lineage. Set to skip lineage altogether.",
)
@click.option(
    "--fail-on-missing-lineage/--auto-add-lineage",
    is_flag=True,
    default=True,
    help=(
        "Default is to fail if lineage documents not present in the database. "
        "Set auto add to try to index lineage documents."
    ),
)
@click.option(
    "--verify-lineage",
    is_flag=True,
    default=False,
    help="Default is no verification. Set to verify parent dataset definitions.",
)
@click.option(
    "--stac",
    is_flag=True,
    default=False,
    help="Expect STAC 1.0 metadata and attempt to transform to ODC EO3 metadata",
)
@click.option(
    "--odc-metadata-link",
    default=None,
    help="Expect metadata doc with ODC EO3 metadata link. "
    "Either provide '/' separated path to find metadata link in a provided "
    "metadata doc e.g. 'foo/bar/link', or if metadata doc is STAC, "
    "provide 'rel' value of the 'links' object having "
    "metadata link. e.g. 'STAC-LINKS-REL:odc_yaml'",
)
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
    "--update-if-exists",
    is_flag=True,
    default=False,
    help="If the dataset already exists, update it instead of skipping it.",
)
@click.option(
    "--archive",
    is_flag=True,
    default=False,
    help="If set, archive datasets",
)
@click.option(
    "--allow-unsafe",
    is_flag=True,
    default=False,
    help="Allow unsafe changes to a dataset. Take care!",
)
@click.option(
    "--record-path",
    default=None,
    multiple=True,
    help="Filtering option for s3 path, i.e. 'L2/sentinel-2-nrt/S2MSIARD/*/*/ARD-METADATA.yaml'",
)
@click.option(
    "--region-code-list-uri",
    default=None,
    help="A path to a list (one item per line, in txt or gzip format) of valide region_codes to include",
)
@click.argument("queue_name", type=str, nargs=1)
@click.argument("product", type=str, nargs=1)
def cli(
    skip_lineage,
    fail_on_missing_lineage,
    verify_lineage,
    stac,
    odc_metadata_link,
    limit,
    update,
    update_if_exists,
    archive,
    allow_unsafe,
    record_path,
    region_code_list_uri,
    queue_name,
    product,
):
    """ Iterate through messages on an SQS queue and add them to datacube"""

    transform = None
    if stac:
        transform = stac_transform

    candidate_products = product.split()

    sqs = boto3.resource("sqs")
    queue = sqs.get_queue_by_name(QueueName=queue_name)

    # Do the thing
    dc = Datacube()
    success, failed = queue_to_odc(
        queue,
        dc,
        candidate_products,
        skip_lineage=skip_lineage,
        fail_on_missing_lineage=fail_on_missing_lineage,
        verify_lineage=verify_lineage,
        transform=transform,
        limit=limit,
        update=update,
        update_if_exists=update_if_exists,
        archive=archive,
        allow_unsafe=allow_unsafe,
        record_path=record_path,
        odc_metadata_link=odc_metadata_link,
        region_code_list_uri=region_code_list_uri,
    )

    result_msg = ""
    if update:
        result_msg += f"Updated {success} Dataset(s), "
    elif archive:
        result_msg += f"Archived {success} Dataset(s), "
    else:
        result_msg += f"Added {success} Dataset(s), "
    result_msg += f"Failed {failed} Dataset(s)"
    print(result_msg)

    if failed > 0:
        sys.exit(failed)


if __name__ == "__main__":
    cli()
