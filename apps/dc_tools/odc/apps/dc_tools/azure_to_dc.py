"""Crawl Thredds for prefixes and fetch YAML's for indexing
and dump them into a Datacube instance
"""
import json
import logging
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List, Optional

import click
from datacube import Datacube
from datacube.index.hl import Doc2Dataset
from odc.apps.dc_tools._stac import stac_transform
from odc.apps.dc_tools.utils import (
    SkippedException,
    allow_unsafe,
    archive_less_mature,
    index_update_dataset,
    publish_action,
    rename_product,
    statsd_gauge_reporting,
    statsd_setting,
    transform_stac,
    update_flag,
    update_if_exists_flag,
)
from odc.azure import download_blob, find_blobs


def stream_blob_urls(account_url, container_name, credential, blobs: List[str]):
    for blob in blobs:
        doc, uri, _ = download_blob(account_url, container_name, credential, blob)
        yield (json.loads(doc), uri)


def process_doc(
    doc,
    uri,
    dc,
    doc2ds,
    stac,
    update,
    update_if_exists,
    allow_unsafe,
    archive_less_mature,
    publish_action,
    rename_product,
):
    stac_doc = None
    if stac:
        stac_doc = doc
        if rename_product is not None:
            # This possibly should be possible for yaml loading too
            doc["properties"]["odc:product"] = rename_product
        doc = stac_transform(doc)
    index_update_dataset(
        doc,
        uri,
        dc,
        doc2ds,
        update=update,
        update_if_exists=update_if_exists,
        allow_unsafe=allow_unsafe,
        archive_less_mature=archive_less_mature,
        publish_action=publish_action,
        stac_doc=stac_doc,
    )


def dump_list_to_odc(
    account_url,
    container_name,
    credential,
    blob_urls: List[str],
    dc: Datacube,
    stac: Optional[bool] = False,
    update: Optional[bool] = False,
    update_if_exists: Optional[bool] = False,
    allow_unsafe: Optional[bool] = False,
    archive_less_mature: Optional[int] = None,
    publish_action: Optional[str] = None,
    rename_product: Optional[str] = None,
):
    doc2ds = Doc2Dataset(dc.index)

    # Do the indexing of all the things
    success = 0
    failure = 0
    skipped = 0

    sys.stdout.write("\rIndexing from Azure...\n")
    with ThreadPoolExecutor(max_workers=50) as executor:
        future_to_item = {
            executor.submit(
                process_doc,
                doc,
                dc,
                doc2ds,
                stac,
                update,
                update_if_exists=update_if_exists,
                allow_unsafe=allow_unsafe,
                rename_product=rename_product,
                archive_less_mature=archive_less_mature,
                publish_action=publish_action,
            ): uri
            for doc, uri in stream_blob_urls(
                account_url, container_name, credential, blob_urls
            )
        }
        for future in as_completed(future_to_item):
            item = future_to_item[future]
            try:
                _ = future.result()
                success += 1
                if success % 10 == 0:
                    sys.stdout.write(f"\rAdded {success} datasets...")
            except SkippedException:
                skipped += 1
            except Exception as e:  # pylint:disable=broad-except
                logging.exception("Failed to handle item %s with exception %s", item, e)
                failure += 1
    sys.stdout.write("\r")

    return success, skipped, failure


@click.command("azure-to-dc")
@update_flag
@update_if_exists_flag
@allow_unsafe
@transform_stac
@statsd_setting
@archive_less_mature
@publish_action
@click.option(
    "--account_url",
    "-a",
    "account_url",
    default=None,
    help=(
        "If you set the account URL, then you need a different"
        "kind of connection string to if you don't set one"
    ),
)
@click.argument("container_name", type=str, nargs=1)
@click.argument("credential", type=str, nargs=1)
@click.argument("prefix", type=str, nargs=1)
@click.argument("suffix", type=str, nargs=1)
@rename_product
def cli(
    update: bool,
    update_if_exists: bool,
    allow_unsafe: bool,
    stac: bool,
    statsd_setting: str,
    archive_less_mature: int,
    publish_action: str,
    account_url: str,
    container_name: str,
    credential: str,
    prefix: str,
    suffix: str,
    rename_product: str,
):
    # Set up the datacube first, to ensure we have a connection
    dc = Datacube()
    print(f"Opening AZ Container {container_name} on {account_url}")
    print(f"Searching on prefix '{prefix}' for files matching suffix '{suffix}'")
    yaml_urls = find_blobs(
        container_name, credential, prefix, suffix, account_url=account_url
    )

    # Consume generator and fetch YAML's
    added, skipped, failed = dump_list_to_odc(
        account_url,
        container_name,
        credential,
        yaml_urls,
        dc,
        stac=stac,
        update=update,
        update_if_exists=update_if_exists,
        allow_unsafe=allow_unsafe,
        archive_less_mature=archive_less_mature,
        publish_action=publish_action,
        rename_product=rename_product,
    )

    print(
        f"Added {added} Datasets, failed {failed} Datasets, skipped {skipped} Datasets"
    )
    if statsd_setting:
        statsd_gauge_reporting(
            added, ["app:azure_to_dc", "action:added"], statsd_setting
        )
        statsd_gauge_reporting(
            failed, ["app:azure_to_dc", "action:failed"], statsd_setting
        )


if __name__ == "__main__":
    cli()
