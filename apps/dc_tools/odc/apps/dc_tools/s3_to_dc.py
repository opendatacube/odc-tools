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
from odc.apps.dc_tools.utils import IndexingException, index_update_dataset
from odc.index.stac import stac_transform
import concurrent
from odc.index import parse_doc_stream


# Grab the URL from the resulting S3 item
def stream_urls(urls):
    for url in urls:
        yield url.url


# Parse documents as they stream through from S3
def stream_docs(documents):
    for document in documents:
        yield (document.url, document.data)


def dump_to_odc(
    document_stream,
    dc: Datacube,
    products: list,
    transform=None,
    update=False,
    update_if_exists=False,
    allow_unsafe=False,
    n_threads=None,
    **kwargs,
) -> Tuple[int, int]:
    doc2ds = Doc2Dataset(dc.index, products=products, **kwargs)

    ds_added = 0
    ds_failed = 0

    uris_docs = parse_doc_stream(
        stream_docs(document_stream), dc.index, transform=transform
    )

    if n_threads:
        logging.info("Starting with {n_threads} threads")
        with concurrent.futures.ThreadPoolExecutor(max_workers=n_threads) as executor:
            future_to_index = {
                executor.submit(
                    index_update_dataset,
                    metadata,
                    uri,
                    dc,
                    doc2ds,
                    update,
                    update_if_exists,
                    allow_unsafe,
                ): uri for uri, metadata in uris_docs
            }
            for future in concurrent.futures.as_completed(future_to_index):
                uri = future_to_index[future]
                try:
                    future.result()
                except Exception as e:
                    logging.error(f"Failed to index dataset {uri} with error {e}")
                    ds_failed += 1
                else:
                    ds_added += 1
    else:
        logging.info("Starting without threading")
        for uri, metadata in uris_docs:
            try:
                index_update_dataset(
                    metadata, uri, dc, doc2ds, update, update_if_exists, allow_unsafe
                )
                ds_added += 1
            except (IndexingException) as e:
                logging.error(f"Failed to index dataset {uri} with error {e}")
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
    "--update-if-exists",
    is_flag=True,
    default=False,
    help="If the dataset already exists, update it instead of skipping it.",
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
    "--no-sign-request",
    is_flag=True,
    default=False,
    help="Do not sign AWS S3 requests.",
)
@click.option(
    "--request-payer",
    is_flag=True,
    default=False,
    help="Needed when accessing requester pays public buckets.",
)
@click.option(
    "--n-threads",
    type=int,
    default=None,
    help="Set as n to use n threads when indexing.",
)
@click.argument("uri", type=str, nargs=1)
@click.argument("product", type=str, nargs=1)
def cli(
    skip_lineage,
    fail_on_missing_lineage,
    verify_lineage,
    stac,
    update,
    update_if_exists,
    allow_unsafe,
    skip_check,
    no_sign_request,
    request_payer,
    n_threads,
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

    added, failed = dump_to_odc(
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
        n_threads=n_threads,
    )

    print(f"Added {added} Datasets, Failed {failed} Datasets")

    if failed > 0:
        sys.exit(failed)


if __name__ == "__main__":
    cli()
