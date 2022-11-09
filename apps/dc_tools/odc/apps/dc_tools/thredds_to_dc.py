"""Crawl Thredds for prefixes and fetch YAML's for indexing
and dump them into a Datacube instance
"""
import logging
import sys
from typing import List, Tuple

import click
from datacube import Datacube
from odc.apps.dc_tools.utils import statsd_gauge_reporting, statsd_setting
from odc.thredds import download_yamls, thredds_find_glob

from ._docs import from_yaml_doc_stream


def dump_list_to_odc(
    yaml_content_list: List[Tuple[bytes, str, str]],
    dc: Datacube,
    products: List[str],
    **kwargs,
):
    expand_stream = (
        ("https://" + d[1], d[0]) for d in yaml_content_list if d[0] is not None
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
            # TODO: Potentially wrap this in transactions and batch to DB
            # TODO: Capture UUID's from YAML and perform a bulk has
            try:
                dc.index.datasets.add(ds)
                ds_added += 1
            except Exception as e:
                logging.error(e)
                ds_failed += 1

    return ds_added, ds_failed


@click.command("thredds-to-dc")
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
@statsd_setting
@click.argument("uri", type=str, nargs=1)
@click.argument("product", type=str, nargs=1)
def cli(
    skip_lineage: bool,
    fail_on_missing_lineage: bool,
    verify_lineage: bool,
    statsd_setting: str,
    uri: str,
    product: str,
):
    skips = [".*NBAR.*", ".*SUPPLEMENTARY.*", ".*NBART.*", ".*/QA/.*"]
    select = [".*ARD-METADATA.yaml"]
    candidate_products = product.split()
    print(f"Crawling {uri} on Thredds")
    print(f"Matching to {candidate_products}")
    yaml_urls = thredds_find_glob(uri, skips, select)
    print(f"Found {len(yaml_urls)} datasets")

    yaml_contents = download_yamls(yaml_urls)

    # Consume generator and fetch YAML's
    dc = Datacube()
    added, failed = dump_list_to_odc(
        yaml_contents,
        dc,
        candidate_products,
        skip_lineage=skip_lineage,
        fail_on_missing_lineage=fail_on_missing_lineage,
        verify_lineage=verify_lineage,
    )

    print(f"Added {added} Datasets, Failed {failed} Datasets")
    if statsd_setting:
        statsd_gauge_reporting(
            added, ["app:thredds_to_dc", "action:added"], statsd_setting
        )
        statsd_gauge_reporting(
            failed, ["app:thredds_to_dc", "action:failed"], statsd_setting
        )
