import json
from pathlib import Path

import click
import datacube
from datacube.index.hl import Doc2Dataset
from odc.apps.dc_tools.utils import (
    index_update_dataset,
    update_if_exists,
    allow_unsafe,
    transform_stac,
    statsd_gauge_reporting, statsd_setting,
)
from ._stac import stac_transform
from typing import Generator, Optional
import logging


import yaml
try:
    from yaml import CLoader as Loader
except ImportError:
    from yaml import Loader

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
@transform_stac
@statsd_setting
@click.option(
    "--glob",
    default=None,
    help="File system glob to use, defaults to **/*.yaml or **/*.json for STAC.",
)
def cli(input_directory, update_if_exists, allow_unsafe, stac, statsd_setting, glob):

    dc = datacube.Datacube()
    doc2ds = Doc2Dataset(dc.index)

    if glob is None:
        glob = "**/*.json" if stac else "**/*.yaml"

    files_to_process = _find_files(input_directory, glob, stac=stac)

    added, failed = 0, 0

    for in_file in files_to_process:
        with in_file.open() as f:
            try:
                if in_file.endswith(".yml") or in_file.endswith(".yaml"):
                    metadata = yaml.safe_load(f, Loader=Loader)
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
                )
                added += 1
            except Exception as e:
                logging.exception(f"Failed to add dataset {in_file} with error {e}")
                failed += 1

    logging.info(f"Added {added} and failed {failed} datasets.")
    if statsd_setting:
        statsd_gauge_reporting('fs_to_dc', added, ["action:added"], statsd_setting)
        statsd_gauge_reporting('fs_to_dc', failed, ["action:failed"], statsd_setting)


if __name__ == "__main__":
    cli()
