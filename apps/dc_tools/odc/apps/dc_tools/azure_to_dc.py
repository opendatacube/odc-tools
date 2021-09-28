import logging

import click
from datacube.index.hl import Doc2Dataset
from odc.azure import find_blobs, download_blob
from datacube import Datacube

from odc.stac.transform import stac_transform

from odc.apps.dc_tools.utils import index_update_dataset, update, update_if_exists, allow_unsafe, transform_stac

from typing import List, Optional
import json


def stream_blob_urls(account_url, container_name, credential, blobs: List[str]):
    for blob in blobs:
        doc, uri, _ = download_blob(account_url, container_name, credential, blob)
        yield (json.loads(doc), uri)


def dump_list_to_odc(
    account_url,
    container_name,
    credential,
    blob_urls: List[str],
    dc: Datacube,
    stac: Optional[bool] = False,
    update: Optional[bool] = False,
    update_if_exists: Optional[bool] = False,
    allow_unsafe: Optional[bool] = False
):
    ds_added = 0
    ds_failed = 0
    doc2ds = Doc2Dataset(dc.index)

    for doc, uri in stream_blob_urls(account_url, container_name, credential, blob_urls):
        try:
            if stac:
                doc = stac_transform(doc)
            index_update_dataset(doc, uri, dc, doc2ds, update=update, update_if_exists=update_if_exists, allow_unsafe=allow_unsafe)
            ds_added += 1
        except Exception as e:
            logging.error(f'Failed to add {uri}')
            logging.exception(e)
            ds_failed += 1

    return ds_added, ds_failed


@click.command("azure-to-dc")
@update
@update_if_exists
@allow_unsafe
@transform_stac
@click.option("--account_url", type=str, required=False, default=None)
@click.argument("container_name", type=str, nargs=1)
@click.argument("credential", type=str, nargs=1)
@click.argument("prefix", type=str, nargs=1)
@click.argument("suffix", type=str, nargs=1)
def cli(
    update: bool,
    update_if_exists: bool,
    allow_unsafe: bool,
    stac: bool,
    account_url: str,
    container_name: str,
    credential: str,
    prefix: str,
    suffix: str,
):
    print(f"Opening AZ Container {container_name} on {account_url}")
    print(f"Searching on prefix '{prefix}' for files matching suffix '{suffix}'")
    yaml_urls = find_blobs(container_name, credential, prefix, suffix, account_url=account_url)

    # Consume generator and fetch YAML's
    dc = Datacube()
    added, failed = dump_list_to_odc(
        account_url,
        container_name,
        credential,
        yaml_urls,
        dc,
        stac=stac,
        update=update,
        update_if_exists=update_if_exists,
        allow_unsafe=allow_unsafe
    )

    print(f"Added {added} Datasets, Failed to add {failed} Datasets")
