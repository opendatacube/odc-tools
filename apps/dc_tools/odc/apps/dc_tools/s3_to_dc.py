#!/usr/bin/env python3
"""Build S3 iterators using odc-tools
and index datasets found into RDS
"""

import logging
import sys
from types import SimpleNamespace
from typing import Any, Dict

import botocore
import click
from datacube import Datacube
from datacube.index.hl import Doc2Dataset
from datacube.ui.click import environment_option, pass_config
from datacube.utils.aws import (
    _aws_unsigned_check_env,
    auto_find_region,
    s3_client,
    s3_fetch,
)
from datacube.utils.documents import parse_doc_stream
from odc.aio import S3Fetcher, s3_find_glob
from odc.apps.dc_tools.utils import (
    DatasetExists,
    IndexingError,
    allow_unsafe,
    archive_less_mature,
    fail_on_missing_lineage,
    index_update_dataset,
    item_to_meta_uri,
    get_self_link,
    no_sign_request,
    publish_action,
    rename_product,
    request_payer,
    skip_check,
    skip_lineage,
    statsd_gauge_reporting,
    statsd_setting,
    transform_stac,
    update_flag,
    update_if_exists_flag,
    url_string_replace,
    verify_lineage,
)
from pystac import Item
from sqlalchemy.exc import OperationalError, ProgrammingError


def doc_error(uri, doc, exc: Exception) -> None:
    """Log the internal errors parsing docs"""
    logging.exception("Failed to parse doc at %s", uri)


class SimpleFetcher:
    """
    Super simple S3 URL fetcher.

    Args:
        region_name (str, optional): AWS region name to use for S3 requests.If not provided, attempts to auto-detect.
        aws_unsigned (bool, optional): If True, disables AWS request signing for public buckets.
        request_opts (dict, optional): Additional options to pass to the S3 fetch operation.

    Methods:
        __call__(uris):
            Fetches a sequence of S3 URLs.
            Args:
                uris (Iterable): Sequence of S3 URLs.
            Yields:
                SimpleNamespace: For each input, yields an object with:
                    - url (str): The S3 URL.
                    - data (bytes or None): The fetched data.
            Notes:
                - The order of results is not guaranteed to match the input order.
                - One result is yielded for each input URI.
    """

    def __init__(
        self,
        region_name: str | None = None,
        aws_unsigned: bool | None = None,
        request_opts: Dict[Any, Any] | None = None,
    ):
        opts = {}

        if request_opts is None:
            request_opts = {}

        if region_name is None:
            region_name = auto_find_region()

        if aws_unsigned is None:
            aws_unsigned = _aws_unsigned_check_env()

        if aws_unsigned:
            opts["signature_version"] = botocore.UNSIGNED

        opts["region_name"] = region_name
        opts["aws_unsigned"] = aws_unsigned

        self.opts = opts
        self.request_opts = request_opts

    def __call__(self, uris):
        for url in uris:
            client = s3_client(**self.opts)
            data = s3_fetch(s3=client, url=url, **self.request_opts)

            yield SimpleNamespace(url=url, data=data)


@click.command("s3-to-dc")
@environment_option
@pass_config
@click.option(
    "--log",
    type=click.Choice(
        ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"], case_sensitive=False
    ),
    default="WARNING",
    show_default=True,
    help="control the log level, e.g., --log=error",
)
@skip_lineage
@fail_on_missing_lineage
@verify_lineage
@transform_stac
@update_flag
@update_if_exists_flag
@allow_unsafe
@skip_check
@no_sign_request
@statsd_setting
@request_payer
@archive_less_mature
@publish_action
@rename_product
@url_string_replace
@click.option(
    "--convert-bools",
    is_flag=True,
    default=False,
    help="Convert boolean properties to strings for backwards compatibility",
)
@click.argument("uris", nargs=-1)
@click.argument("product", type=str, nargs=1, required=False)
def cli(
    cfg_env,
    log: str,
    skip_lineage: bool,
    fail_on_missing_lineage: bool,
    verify_lineage: bool,
    stac: bool,
    update: bool,
    update_if_exists: bool,
    allow_unsafe: bool,
    skip_check: bool,
    no_sign_request: bool,
    statsd_setting: str,
    request_payer: bool,
    archive_less_mature: int | None,
    publish_action: str,
    rename_product: str | None,
    url_string_replace: str | None,
    convert_bools: bool,
    uris: list[str],
    product: str,
) -> None:
    """
    Iterate through files in an S3 bucket and add them to datacube.

    File uris can be provided as a glob, or as a list of absolute URLs.
    If more than one uri is given, all will be treated as absolute URLs.

    Product is optional; if one is provided, it must match all datasets.
    Can provide a single product name or a space separated list of multiple products
    (formatted as a single string).
    """
    log_level = getattr(logging, log.upper())
    logging.basicConfig(
        level=log_level,
        format="%(asctime)s: %(levelname)s: %(message)s",
        datefmt="%m/%d/%Y %I:%M:%S",
    )

    opts = {}
    if request_payer:
        opts["RequestPayer"] = "requester"

    try:
        dc = Datacube(env=cfg_env, app="s3-to-dc")
    except (OperationalError, ProgrammingError) as e:
        print(f"ERROR: {e}", file=sys.stderr)
        sys.exit(1)
    # if it's a uri, a product wasn't provided, and 'product' is actually another uri
    if product.startswith("s3://"):
        candidate_products = []
        uris += (product,)
    else:
        # Check datacube connection and products
        candidate_products = product.split()
        odc_products = set(dc.list_products().name.values)
        if not set(candidate_products).issubset(odc_products):
            missing_products = list(set(candidate_products) - odc_products)
            print(
                f"Error: Requested Product/s {', '.join(missing_products)} "
                f"{'is' if len(missing_products) == 1 else 'are'} "
                "not present in the ODC Database",
                file=sys.stderr,
            )
            sys.exit(1)

    is_glob = True
    # we assume the uri to be an absolute URL if it contains no wildcards
    # or if there are multiple uri values provided
    if (len(uris) > 1) or ("*" not in uris[0]):
        is_glob = False
        for url in uris:
            if "*" in url:
                logging.warning(
                    "A list of uris is assumed to include only absolute URLs. "
                    "Any wildcard characters will be escaped."
                )
    # Get a generator from supplied S3 Uri for candidate documents
    # Grab the URL from the resulting S3 item
    if is_glob:
        fetcher = S3Fetcher(aws_unsigned=no_sign_request)
        document_stream = fetcher(
            url.url
            for url in s3_find_glob(uris[0], skip_check=skip_check, s3=fetcher, **opts)
        )
    else:
        # if working with absolute URLs, no need for all the globbing logic
        document_stream = SimpleFetcher(
            aws_unsigned=no_sign_request, request_opts=opts
        )(uris)

    if url_string_replace:
        url_string_replace_tuple = tuple(url_string_replace.split(","))
        if len(url_string_replace_tuple) != 2:
            print(
                "url_string_replace must be two strings separated by a comma",
                file=sys.stderr,
            )
            sys.exit(1)
    else:
        url_string_replace_tuple = None

    doc2ds = Doc2Dataset(
        dc.index,
        products=candidate_products,
        skip_lineage=skip_lineage,
        fail_on_missing_lineage=fail_on_missing_lineage,
        verify_lineage=verify_lineage,
    )

    added = 0
    failed = 0
    skipped = 0
    found_docs = False
    try:
        for uri, dataset in parse_doc_stream(
            ((doc.url, doc.data) for doc in document_stream), on_error=doc_error
        ):
            if dataset is None:
                skipped += 1
                continue
            found_docs = True
            if convert_bools:
                for prop, val in dataset["properties"].items():
                    if val is True:
                        dataset["properties"][prop] = "true"
                    elif val is False:
                        dataset["properties"][prop] = "false"
            stac_doc = None
            if stac:
                item = Item.from_dict(dataset)
                if get_self_link(item) is None:
                    item.set_self_href(uri)
                dataset, new_uri, stac_doc = item_to_meta_uri(
                    item,
                    dc,
                    rename_product=rename_product,
                    url_string_replace=url_string_replace_tuple,
                )
                uri = new_uri or uri
            try:
                index_update_dataset(
                    dataset,
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
                added += 1
            except IndexingError:
                logging.exception("Failed to index dataset %s", uri)
                failed += 1
            except DatasetExists:
                skipped += 1
    except OSError as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)

    if not found_docs:
        print("Doc stream was empty", file=sys.stderr)
        sys.exit(1)

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
