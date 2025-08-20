#!/usr/bin/env python3
"""Build S3 iterators using odc-tools
and index datasets found into RDS
"""

import logging
import sys
from types import SimpleNamespace
from typing import Any, Dict, Tuple

import botocore
import click
from datacube import Datacube
from datacube.index.hl import Doc2Dataset
from datacube.ui.click import environment_option, pass_config
from odc.aio import S3Fetcher, s3_find_glob
from odc.apps.dc_tools._docs import parse_doc_stream
from odc.apps.dc_tools.utils import (
    IndexingException,
    SkippedException,
    allow_unsafe,
    archive_less_mature,
    fail_on_missing_lineage,
    index_update_dataset,
    item_to_meta_uri,
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
from odc.aws import _aws_unsigned_check_env, auto_find_region, s3_client, s3_fetch
from pystac import Item


def doc_error(uri, doc) -> None:
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


def dump_to_odc(
    document_stream,
    dc: Datacube,
    products: list,
    transform=None,
    update=False,
    update_if_exists=False,
    allow_unsafe=False,
    archive_less_mature=None,
    publish_action=None,
    rename_product: None | str = None,
    url_string_replace: None | tuple[str, str] | None = None,
    convert_bools: bool = False,
    **kwargs,
) -> Tuple[int, int, int]:
    doc2ds = Doc2Dataset(dc.index, products=products, **kwargs)

    ds_added = 0
    ds_failed = 0
    ds_skipped = 0
    uris_docs = parse_doc_stream(
        ((doc.url, doc.data) for doc in document_stream),
        on_error=doc_error,
    )

    found_docs = False
    for uri, dataset in uris_docs:
        if dataset is None:
            ds_skipped += 1
            continue
        found_docs = True
        stac = None
        if convert_bools:
            for prop, val in dataset["properties"].items():
                if val is True:
                    dataset["properties"][prop] = "true"
                elif val is False:
                    dataset["properties"][prop] = "false"
        if transform:
            item = Item.from_dict(dataset)
            dataset, uri, stac = item_to_meta_uri(
                item,
                dc,
                rename_product=rename_product,
                url_string_replace=url_string_replace,
            )
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
                stac_doc=stac,
            )
            ds_added += 1
        except IndexingException:
            logging.exception("Failed to index dataset %s", uri)
            ds_failed += 1
        except SkippedException:
            ds_skipped += 1
    if not found_docs:
        raise click.ClickException("Doc stream was empty")

    return ds_added, ds_failed, ds_skipped


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
    log,
    skip_lineage,
    fail_on_missing_lineage,
    verify_lineage,
    stac,
    update,
    update_if_exists,
    allow_unsafe,
    skip_check,
    no_sign_request,
    statsd_setting,
    request_payer,
    archive_less_mature,
    publish_action,
    rename_product,
    url_string_replace,
    convert_bools,
    uris,
    product,
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

    dc = Datacube(env=cfg_env)

    # if it's a uri, a product wasn't provided, and 'product' is actually another uri
    if product.startswith("s3://"):
        candidate_products = []
        uris += (product,)
    else:
        # Check datacube connection and products
        candidate_products = product.split()
        odc_products = dc.list_products().name.values

        odc_products = set(odc_products)
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
    fetcher = None
    # Grab the URL from the resulting S3 item
    if is_glob:
        fetcher = S3Fetcher(aws_unsigned=no_sign_request)
        document_stream = (
            url.url
            for url in s3_find_glob(uris[0], skip_check=skip_check, s3=fetcher, **opts)
        )
    else:
        # if working with absolute URLs, no need for all the globbing logic
        fetcher = SimpleFetcher(
            aws_unsigned=no_sign_request,
            request_opts=opts,
        )
        document_stream = uris

    if url_string_replace:
        url_string_replace_tuple = tuple(url_string_replace.split(","))
        if len(url_string_replace_tuple) != 2:
            raise ValueError(
                "url_string_replace must be two strings separated by a comma"
            )
    else:
        url_string_replace_tuple = None

    added, failed, skipped = dump_to_odc(
        fetcher(document_stream),
        dc,
        candidate_products,
        skip_lineage=skip_lineage,
        fail_on_missing_lineage=fail_on_missing_lineage,
        verify_lineage=verify_lineage,
        transform=stac,
        update=update,
        update_if_exists=update_if_exists,
        allow_unsafe=allow_unsafe,
        archive_less_mature=archive_less_mature,
        publish_action=publish_action,
        rename_product=rename_product,
        url_string_replace=url_string_replace_tuple,
        convert_bools=convert_bools,
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
