#!/usr/bin/env python3
"""Build S3 iterators using odc-tools
and index datasets found into RDS
"""
import click
import logging
import sys
from odc.aio import S3Fetcher, s3_find_glob
from typing import Tuple

from datacube import Datacube
from datacube.index.hl import Doc2Dataset
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
    update_flag,
    update_if_exists_flag,
    verify_lineage,
    publish_action,
)

logging.basicConfig(
    level=logging.WARNING,
    format="%(asctime)s: %(levelname)s: %(message)s",
    datefmt="%m/%d/%Y %I:%M:%S",
)


def doc_error(uri, doc):
    """Log the internal errors parsing docs"""
    logging.exception("Failed to parse doc at %s", uri)


def dump_to_odc(
    document_stream,
    dc: Datacube,
    product: list,
    transform=None,
    update=False,
    update_if_exists=False,
    allow_unsafe=False,
    archive_less_mature=None,
    publish_action=None,
    **kwargs,
) -> Tuple[int, int, int]:
    doc2ds = Doc2Dataset(dc.index, products=product, **kwargs)

    ds_added = 0
    ds_failed = 0
    ds_skipped = 0
    uris_docs = parse_doc_stream(
        ((doc.url, doc.data) for doc in document_stream),
        on_error=doc_error,
        transform=transform,
    )

    found_docs = False
    for uri, metadata in uris_docs:
        found_docs = True
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
                publish_action=publish_action,
            )
            ds_added += 1
        except IndexingException:
            logging.exception("Failed to index dataset %s", uri)
            ds_failed += 1
        except SkippedException:
            ds_skipped += 1
    if not found_docs:
        raise click.Abort("Doc stream was empty")

    return ds_added, ds_failed, ds_skipped


@click.command("s3-to-dc")
@skip_lineage
@fail_on_missing_lineage
@verify_lineage
@transform_stac
@transform_stac_absolute
@update_flag
@update_if_exists_flag
@allow_unsafe
@skip_check
@no_sign_request
@statsd_setting
@request_payer
@archive_less_mature
@publish_action
@click.argument("uri", type=str, nargs=-1)
@click.argument("product", type=str, nargs=1, required=False)
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
    publish_action,
    uri,
    product,
):
    """
    Iterate through files in an S3 bucket and add them to datacube.

    File uris can be provided as a glob, or as a list of absolute URLs.
    If more than one uri is given, all will be treated as absolute URLs.

    Product is optional; if one is provided, it must match all datasets.
    Only one product can be provided.
    """

    transform = None
    if stac:
        if absolute:
            transform = stac_transform_absolute
        else:
            transform = stac_transform

    opts = {}
    if request_payer:
        opts["RequestPayer"] = "requester"

    dc = Datacube()
    logging.warning(f"product is: {product}")

    # if it's a uri, a product wasn't provided, and 'product' is actually another uri
    if product.startswith("s3://"):
        candidate_product = []
        if isinstance(uri, str):
            uri = [uri, product]
        else:
            uri = list(uri)
            uri.append(product)
    else:
        # Check datacube connection and products
        candidate_product = product.split()
        odc_products = dc.list_products().name.values

        odc_products = set(odc_products)
        if not set(candidate_product).issubset(odc_products):
            print(
                f"Error: Requested Product {product} is not present in the ODC Database",
                file=sys.stderr,
            )
            sys.exit(1)

    is_glob = True
    # we assume the uri to be an absolute URL if it contains no wildcards
    # or if there are multiple uri values provided
    if (len(uri) > 1) or ("*" not in uri[0]):
        is_glob = False
        for url in uri:
            if "*" in url:
                logging.warning(
                    "A list of uris is assumed to include only absolute URLs. "
                    "Any wildcard characters will be escaped."
                )

    # Get a generator from supplied S3 Uri for candidate documents
    fetcher = S3Fetcher(aws_unsigned=no_sign_request)
    # Grab the URL from the resulting S3 item
    if is_glob:
        document_stream = (
            url.url
            for url in s3_find_glob(uri[0], skip_check=skip_check, s3=fetcher, **opts)
        )
    else:
        # if working with absolute URLs, no need for all the globbing logic
        document_stream = uri

    added, failed, skipped = dump_to_odc(
        fetcher(document_stream),
        dc,
        candidate_product,
        skip_lineage=skip_lineage,
        fail_on_missing_lineage=fail_on_missing_lineage,
        verify_lineage=verify_lineage,
        transform=transform,
        update=update,
        update_if_exists=update_if_exists,
        allow_unsafe=allow_unsafe,
        archive_less_mature=archive_less_mature,
        publish_action=publish_action,
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
