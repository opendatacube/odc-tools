import click
import json
import logging
import yaml
from pathlib import Path

import datacube
from datacube.index.hl import Doc2Dataset
from odc.apps.dc_tools._stac import stac_transform
from odc.apps.dc_tools.utils import (
    allow_unsafe,
    archive_less_mature,
    index_update_dataset,
    update_if_exists_flag,
    publish_action,
    statsd_server,
    report_statsd_gauge,
    transform_stac,
)

logging.basicConfig(
    level=logging.WARNING,
    format="%(asctime)s: %(levelname)s: %(message)s",
    datefmt="%m/%d/%Y %I:%M:%S",
)


@click.command("fs-to-dc")
@click.argument("input_directory", type=str, nargs=1)
@update_if_exists_flag
@allow_unsafe
@archive_less_mature
@transform_stac
@statsd_server
@publish_action
@click.option(
    "--glob",
    default=None,
    help="File system glob to use, defaults to **/*.yaml or **/*.json for STAC.",
)
def cli(
    input_directory,
    update_if_exists,
    allow_unsafe,
    stac,
    statsd_server,
    glob,
    archive_less_mature,
    publish_action,
):
    dc = datacube.Datacube()
    doc2ds = Doc2Dataset(dc.index)

    if glob is None:
        glob = "**/*.json" if stac else "**/*.yaml"

    files_to_process = Path(input_directory).glob(glob)

    added, failed = 0, 0

    for in_file in files_to_process:
        with in_file.open() as f:
            try:
                if in_file.suffix in (".yml", ".yaml"):
                    metadata = yaml.safe_load(f)
                else:
                    metadata = json.load(f)
                # Do the STAC Transform if it's flagged
                stac_doc = None
                if stac:
                    stac_doc = metadata
                    metadata = stac_transform(metadata)
                index_update_dataset(
                    metadata,
                    in_file.absolute().as_uri(),
                    dc=dc,
                    doc2ds=doc2ds,
                    update_if_exists=update_if_exists,
                    allow_unsafe=allow_unsafe,
                    archive_less_mature=archive_less_mature,
                    publish_action=publish_action,
                    stac_doc=stac_doc,
                )
                added += 1
            except Exception:  # pylint: disable=broad-except
                logging.exception("Failed to add dataset %s", in_file)
                failed += 1

    logging.info("Added %s and failed %s datasets.", added, failed)
    if statsd_server:
        report_statsd_gauge(added, ["app:fs_to_dc", "action:added"], statsd_server)
        report_statsd_gauge(failed, ["app:fs_to_dc", "action:failed"], statsd_server)


if __name__ == "__main__":
    cli()
