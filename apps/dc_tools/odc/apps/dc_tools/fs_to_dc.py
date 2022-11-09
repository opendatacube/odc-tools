import json
import logging
from pathlib import Path
from typing import Generator, Optional

import click
import datacube
import yaml
from datacube.index.hl import Doc2Dataset
from odc.apps.dc_tools._stac import stac_transform
from odc.apps.dc_tools.utils import (
    allow_unsafe,
    archive_less_mature,
    index_update_dataset,
    statsd_gauge_reporting,
    statsd_setting,
    transform_stac,
    update_if_exists,
)

logging.basicConfig(
    level=logging.WARNING,
    format="%(asctime)s: %(levelname)s: %(message)s",
    datefmt="%m/%d/%Y %I:%M:%S",
)


def _find_files(
    path: str, glob: Optional[str] = None, stac: Optional[bool] = False
) -> Generator[Path, None, None]:
    if glob is None:
        glob = "**/*.json" if stac else "**/*.yaml"

    return Path(path).glob(glob)


@click.command("fs-to-dc")
@click.argument("input_directory", type=str, nargs=1)
@update_if_exists
@allow_unsafe
@archive_less_mature
@transform_stac
@statsd_setting
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
    statsd_setting,
    glob,
    archive_less_mature,
):

    dc = datacube.Datacube()
    doc2ds = Doc2Dataset(dc.index)

    if glob is None:
        glob = "**/*.json" if stac else "**/*.yaml"

    files_to_process = _find_files(input_directory, glob, stac=stac)

    added, failed = 0, 0

    for in_file in files_to_process:
        with in_file.open() as f:
            try:
                if in_file.suffix == ".yml" or in_file.suffix == ".yaml":
                    metadata = yaml.safe_load(f)
                else:
                    metadata = json.load(f)
                # Do the STAC Transform if it's flagged
                if stac:
                    metadata = stac_transform(metadata)
                index_update_dataset(
                    metadata,
                    in_file.absolute().as_uri(),
                    dc=dc,
                    doc2ds=doc2ds,
                    update_if_exists=update_if_exists,
                    allow_unsafe=allow_unsafe,
                    archive_less_mature=archive_less_mature,
                )
                added += 1
            except Exception as e:
                logging.exception(f"Failed to add dataset {in_file} with error {e}")
                failed += 1

    logging.info(f"Added {added} and failed {failed} datasets.")
    if statsd_setting:
        statsd_gauge_reporting(added, ["app:fs_to_dc", "action:added"], statsd_setting)
        statsd_gauge_reporting(
            failed, ["app:fs_to_dc", "action:failed"], statsd_setting
        )


if __name__ == "__main__":
    cli()
