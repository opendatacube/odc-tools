"""Crawl Thredds for prefixes and fetch YAML's for indexing
and dump them into a Datacube instance
"""
import sys
import logging
from typing import List, Tuple

import click
from datacube import Datacube
from odc.apps.dc_tools.utils import statsd_gauge_reporting, statsd_setting
from odc.azure import download_yamls, find_blobs

from ._docs import from_yaml_doc_stream


def dump_list_to_odc(
    account_url,
    container_name,
    yaml_content_list: List[Tuple[bytes, str, str]],
    dc: Datacube,
    products: List[str],
    **kwargs,
):
    expand_stream = (
        (account_url + "/" + container_name + "/" + d[1][: d[1].rfind("/") + 1], d[0])
        for d in yaml_content_list
        if d[0] is not None
    )

    ds_stream = from_yaml_doc_stream(
        expand_stream, dc.index, products=products, **kwargs
    )
    ds_added = 0
    ds_failed = 0
    # Consume chained streams to DB
    for result in ds_stream:
        ds, err = result
        if err is not None:
            logging.error(err)
            ds_failed += 1
        else:
            logging.info(ds)
            try:
                dc.index.datasets.add(ds)
                ds_added += 1
            except Exception as e:
                logging.exception(e)
                ds_failed += 1

    return ds_added, ds_failed


@click.command("azure-to-dc")
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
    "--product",
    "-p",
    "product_names",
    help=(
        "Only match against products specified with this option, "
        "you can supply several by repeating this option with a new product name"
    ),
    multiple=True,
)
@statsd_setting
@click.argument("account_url", type=str, nargs=1)
@click.argument("container_name", type=str, nargs=1)
@click.argument("credential", type=str, nargs=1)
@click.argument("prefix", type=str, nargs=1)
@click.argument("suffix", type=str, nargs=1)
def cli(
    skip_lineage: bool,
    fail_on_missing_lineage: bool,
    verify_lineage: bool,
    product_names: List[str],
    statsd_setting: str,
    account_url: str,
    container_name: str,
    credential: str,
    prefix: str,
    suffix: str,
):
    print(f"Opening AZ Container {container_name} on {account_url}")
    print(f"Searching on prefix '{prefix}' for files matching suffix '{suffix}'")
    yaml_urls = list(find_blobs(container_name, credential, prefix, suffix, account_url))
    n_urls = len(yaml_urls)

    if n_urls != 0:
        print(f"Found {len(yaml_urls)} datasets")
    else:
        print("Didn't find any files to index... finishing")
        sys.exit(1)

    yaml_contents = download_yamls(yaml_urls)

    print(f"Matching to {product_names} products")
    # Consume generator and fetch YAML's
    dc = Datacube()
    added, failed = dump_list_to_odc(
        account_url,
        container_name,
        yaml_contents,
        dc,
        product_names,
        skip_lineage=skip_lineage,
        fail_on_missing_lineage=fail_on_missing_lineage,
        verify_lineage=verify_lineage,
    )

    print(f"Added {added} Datasets, Failed to add {failed} Datasets")
    if statsd_setting:
        statsd_gauge_reporting(
            added, ["app:azure_to_dc", "action:added"], statsd_setting
        )
        statsd_gauge_reporting(
            failed, ["app:azure_to_dc", "action:failed"], statsd_setting
        )
