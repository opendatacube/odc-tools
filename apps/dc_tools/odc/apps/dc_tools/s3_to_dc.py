#!/usr/bin/env python3
"""Build S3 iterators using odc-tools
and index datasets found into RDS
"""
import logging
import sys
from typing import Tuple

import click
from datacube import Datacube
from datacube.index.hl import Doc2Dataset
from odc.aio import S3Fetcher, s3_find_glob
from odc.apps.dc_tools._docs import parse_doc_stream
from odc.apps.dc_tools._stac import stac_transform, stac_transform_absolute
from odc.apps.dc_tools.utils import (
    IndexingException,
    SkippedException,
    allow_unsafe,
    archive_less_mature,
    fail_on_missing_lineage,
    index_update_dataset,
    no_sign_request,
    request_payer,
    skip_check,
    skip_lineage,
    statsd_gauge_reporting,
    statsd_setting,
    transform_stac,
    transform_stac_absolute,
    update,
    update_if_exists,
    verify_lineage,
)


# Grab the URL from the resulting S3 item
def stream_urls(urls):
    for url in urls:
        yield url.url


# Parse documents as they stream through from S3
def stream_docs(documents):
    for document in documents:
        yield (document.url, document.data)


# Log the internal errors parsing docs
def doc_error(uri, doc, e):
    logging.exception(f"Failed to parse doc {uri} with error {e}")


def dump_to_odc(
    document_stream,
    dc: Datacube,
    products: list,
    transform=None,
    update=False,
    update_if_exists=False,
    allow_unsafe=False,
    archive_less_mature=None,
    **kwargs,
) -> Tuple[int, int]:
    doc2ds = Doc2Dataset(dc.index, products=products, **kwargs)

    ds_added = 0
    ds_failed = 0
    ds_skipped = 0
    uris_docs = parse_doc_stream(
        stream_docs(document_stream), on_error=doc_error, transform=transform
    )

    for uri, metadata in uris_docs:
        try:
            index_update_dataset(
                metadata,
                uri,
                dc,
                doc2ds,
                update=update,
                update_if_exists=update_if_exists,
                allow_unsafe=allow_unsafe,
                archive_less_mature=archive_less_mature,
            )
            ds_added += 1
        except IndexingException as e:
            logging.exception(f"Failed to index dataset {uri} with error {e}")
            ds_failed += 1
        except SkippedException:
            ds_skipped += 1

    return ds_added, ds_failed, ds_skipped


@click.command("s3-to-dc")
@skip_lineage
@fail_on_missing_lineage
@verify_lineage
@transform_stac
@transform_stac_absolute
@update
@update_if_exists
@allow_unsafe
@skip_check
@no_sign_request
@statsd_setting
@request_payer
@archive_less_mature
@click.argument("uri", type=str, nargs=1)
@click.argument("product", type=str, nargs=1)
def cli(
    skip_lineage,
    fail_on_missing_lineage,
    verify_lineage,
    stac,
    absolute,
    update,
    update_if_exists,
    allow_unsafe,
    skip_check,
    no_sign_request,
    statsd_setting,
    request_payer,
    archive_less_mature,
    uri,
    product,
):
    """Iterate through files in an S3 bucket and add them to datacube"""

    transform = None
    if stac:
        if absolute:
            transform = stac_transform_absolute
        else:
            transform = stac_transform

    candidate_products = product.split()

    opts = {}
    if request_payer:
        opts["RequestPayer"] = "requester"

    # Check datacube connection and products
    dc = Datacube()
    odc_products = dc.list_products().name.values

    odc_products = set(odc_products)
    if not set(candidate_products).issubset(odc_products):
        missing_products = list(set(candidate_products) - odc_products)
        print(
            f"Error: Requested Product/s {', '.join(missing_products)} {'is' if len(missing_products) == 1 else 'are'} "
            "not present in the ODC Database",
            file=sys.stderr,
        )
        sys.exit(1)

    # Get a generator from supplied S3 Uri for candidate documents
    fetcher = S3Fetcher(aws_unsigned=no_sign_request)
    document_stream = stream_urls(
        s3_find_glob(uri, skip_check=skip_check, s3=fetcher, **opts)
    )

    added, failed, skipped = dump_to_odc(
        fetcher(document_stream),
        dc,
        candidate_products,
        skip_lineage=skip_lineage,
        fail_on_missing_lineage=fail_on_missing_lineage,
        verify_lineage=verify_lineage,
        transform=transform,
        update=update,
        update_if_exists=update_if_exists,
        allow_unsafe=allow_unsafe,
        archive_less_mature=archive_less_mature,
    )

    print(
        f"Added {added} datasets, skipped {skipped} datasets and failed {failed} datasets."
    )
    if statsd_setting:
        statsd_gauge_reporting(added, ["app:s3_to_dc", "action:added"], statsd_setting)
        statsd_gauge_reporting(
            skipped, ["app:s3_to_dc", "action:skipped"], statsd_setting
        )
        statsd_gauge_reporting(
            failed, ["app:s3_to_dc", "action:failed"], statsd_setting
        )

    if failed > 0:
        sys.exit(failed)


if __name__ == "__main__":
    cli()
