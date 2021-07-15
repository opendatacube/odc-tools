#!/usr/bin/env python3
"""Build S3 iterators using odc-tools
and index datasets found into RDS
"""
import logging
import sys
from typing import Tuple
import time
import click
import asyncio
import cProfile
import pstats
from concurrent.futures import ThreadPoolExecutor, as_completed
from datacube import Datacube
from datacube.index.hl import Doc2Dataset
from odc.aio import S3Fetcher, s3_find_glob
from odc.apps.dc_tools.utils import IndexingException, index_update_dataset
from odc.index.stac import stac_transform

from odc.index import parse_doc_stream


# Grab the URL from the resulting S3 item
def stream_urls(urls):
    for url in urls:
        yield url.url


# Parse documents as they stream through from S3
def stream_docs(documents):
    for document in documents:
        yield document.url, document.data


def dump_to_odc(
    document_stream,
    dc: Datacube,
    products: list,
    transform=None,
    update=False,
    update_if_exists=False,
    allow_unsafe=False,
    **kwargs,
) -> Tuple[int, int]:
    doc2ds = Doc2Dataset(dc.index, products=products, **kwargs)

    ds_added = 0
    ds_failed = 0
    uris_docs = parse_doc_stream(stream_docs(document_stream), dc.index, transform=transform)

    for uri, metadata in uris_docs:
        try:
            index_update_dataset(metadata, uri, dc, doc2ds, update, update_if_exists, allow_unsafe)
            ds_added += 1
        except IndexingException as e:
            logging.error(f"Failed to index dataset {uri} with error {e}")
            ds_failed += 1

    return ds_added, ds_failed


def dump_to_odc_thread(
    document_stream,
    dc: Datacube,
    products: list,
    transform=None,
    update=False,
    update_if_exists=False,
    allow_unsafe=False,
    n_threads=100,
    **kwargs,
) -> Tuple[int, int]:
    doc2ds = Doc2Dataset(dc.index, products=products, **kwargs)

    uris_docs = parse_doc_stream(stream_docs(document_stream), dc.index, transform=transform)

    def execute_index_update_dataset(metadata, uri, datacube, doc_to_ds, updated, if_exists_update, unsafe):
        try:
            index_update_dataset(metadata, uri, datacube, doc_to_ds, updated, if_exists_update, unsafe)
            return True
        except IndexingException as index_exception:
            logging.error(f"Failed to index dataset {uri} with error {index_exception}")
            return False

    with ThreadPoolExecutor(max_workers=n_threads) as executor:
        print(f"Indexing datasets over {n_threads} threads")

        tasks = [
            executor.submit(
                execute_index_update_dataset,
                metadata,
                uri,
                dc,
                doc2ds,
                update,
                update_if_exists,
                allow_unsafe,
            )
            for uri, metadata in uris_docs
        ]
        
        result = [future.result() for future in as_completed(tasks)]
        
        print("Finished indexing")
        
    return result.count(True), result.count(False)


async def dump_to_odc_asyncio(
        document_stream,
        dc: Datacube,
        products: list,
        transform=None,
        update=False,
        update_if_exists=False,
        allow_unsafe=False,
        **kwargs,
) -> Tuple[int, int]:

    print("Async call started")

    doc2ds = Doc2Dataset(dc.index, products=products, **kwargs)

    uris_docs = parse_doc_stream(stream_docs(document_stream), dc.index, transform=transform)

    async def execute_index_update_dataset(metadata, uri, datacube, doc_to_ds, updated, if_exists_update, unsafe):
        try:
            index_update_dataset(metadata, uri, datacube, doc_to_ds, updated, if_exists_update, unsafe)
            return True
        except IndexingException as index_exception:
            logging.error(f"Failed to index dataset {uri} with error {index_exception}")
            return False

    result = await asyncio.gather(
        *(
            execute_index_update_dataset(
                metadata,
                uri,
                dc,
                doc2ds,
                update,
                update_if_exists,
                allow_unsafe
            )
            for uri, metadata in uris_docs
        )
    )

    print("Finished indexing")

    return result.count(True), result.count(False)


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
    "--no-sign-request", is_flag=True, default=False, help="Do not sign AWS S3 requests"
)
@click.option(
    "--request-payer",
    is_flag=True,
    default=False,
    help="Needed when accessing requester pays public buckets",
)
@click.option('--n-threads', help='Needed when using multithreading to perform better')
@click.option('--asynk', default=False, help='Test')
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
    asynk,
    uri,
    product,
):
    """ Iterate through files in an S3 bucket and add them to datacube"""
    # Time the process
    s = time.perf_counter()
    # Finding bottlenecks
    profile = cProfile.Profile()
    profile.enable()

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
    document_stream = stream_urls(s3_find_glob(uri, skip_check=skip_check, s3=fetcher, **opts))

    if n_threads:
        added, failed = dump_to_odc_thread(
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
            n_threads=n_threads
        )
    elif asynk:
        added, failed = asyncio.run(
            dump_to_odc_asyncio(
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
            )
        )
    else:
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
        )

    print(f"Added {added} Datasets, Failed {failed} Datasets")
    elapsed = time.perf_counter() - s
    print(f"{__file__} executed in {elapsed:0.2f} seconds.")

    # PRINTING STATUS
    profile.disable()
    ps = pstats.Stats(profile)
    ps.print_stats()

    if failed > 0:
        sys.exit(failed)


if __name__ == "__main__":
    cli()
