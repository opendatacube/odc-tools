#!/usr/bin/env python3
"""Build S3 iterators using odc-tools
and index datasets found into RDS
"""
import logging
import sys
from typing import Tuple

import click
from datacube import Datacube
from datacube.utils import changes
from odc.aio import S3Fetcher, s3_find_glob
from odc.index import from_yaml_doc_stream
from odc.index.stac import stac_transform


def dump_to_odc(
    data_stream,
    dc: Datacube,
    products: list,
    transform=None,
    update=False,
    allow_unsafe=False,
    **kwargs,
) -> Tuple[int, int]:
    # TODO: Get right combination of flags for **kwargs in low validation/no-lineage mode
    expand_stream = ((d.url, d.data) for d in data_stream if d.data is not None)

    ds_stream = from_yaml_doc_stream(
        expand_stream, dc.index, products=products, transform=transform, **kwargs
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
            # TODO: Capture UUID's from dataset doc and perform a bulk has
            try:
                if update:
                    updates = {}
                    if allow_unsafe:
                        updates = {tuple(): changes.allow_any}
                    dc.index.datasets.update(ds, updates_allowed=updates)
                else:
                    dc.index.datasets.add(ds)
                ds_added += 1
            except Exception as e:
                logging.error(e)
                ds_failed += 1

    return ds_added, ds_failed


@click.command("s3-to-dc")
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
    "--update",
    is_flag=True,
    default=False,
    help="If set, update instead of add datasets",
)
@click.option(
    "--allow-unsafe",
    is_flag=True,
    default=False,
    help="Allow unsafe changes to a dataset. Take care!",
)
@click.option(
    "--skip-check",
    is_flag=True,
    default=False,
    help="Assume file exists when listing exact file rather than wildcard.",
)
@click.option(
    "--no-sign-request", is_flag=True, default=False, help="Do not sign AWS S3 requests"
)
@click.option(
    "--request-payer",
    is_flag=True,
    default=False,
    help="Needed when accessing requester pays public buckets",
)
@click.argument("uri", type=str, nargs=1)
@click.argument("product", type=str, nargs=1)
def cli(
    skip_lineage,
    fail_on_missing_lineage,
    verify_lineage,
    stac,
    update,
    allow_unsafe,
    skip_check,
    no_sign_request,
    request_payer,
    uri,
    product,
):
    """ Iterate through files in an S3 bucket and add them to datacube"""

    transform = None
    if stac:
        transform = stac_transform

    candidate_products = product.split()

    opts = {}
    if request_payer:
        opts["RequestPayer"] = "requester"

    # Get a generator from supplied S3 Uri for metadata definitions
    fetcher = S3Fetcher(aws_unsigned=no_sign_request)

    # TODO: Share Fetcher
    s3_obj_stream = s3_find_glob(uri, skip_check=skip_check, s3=fetcher, **opts)

    # Extract URLs from output of iterator before passing to Fetcher
    s3_url_stream = (o.url for o in s3_obj_stream)

    # TODO: Capture S3 URL's in batches and perform bulk_location_has

    # Consume generator and fetch YAML's
    dc = Datacube()
    odc_products = dc.list_products().name.values

    odc_products = set(odc_products)
    if not set(candidate_products).issubset(odc_products):
        missing_products = list(set(candidate_products) - odc_products)
        print(f"Error: Requested Product/s {', '.join(missing_products)} {'is' if len(missing_products) == 1 else 'are'} "
               "not present in the ODC Database", file=sys.stderr)
        sys.exit(1)

    added, failed = dump_to_odc(
        fetcher(s3_url_stream),
        dc,
        candidate_products,
        skip_lineage=skip_lineage,
        fail_on_missing_lineage=fail_on_missing_lineage,
        verify_lineage=verify_lineage,
        transform=transform,
        update=update,
        allow_unsafe=allow_unsafe,
    )

    print(f"Added {added} Datasets, Failed {failed} Datasets")


if __name__ == "__main__":
    cli()
