import click
import json
import logging
import sys
import yaml
from pathlib import Path
import pystac

import datacube
from datacube.index.hl import Doc2Dataset
from datacube.metadata import stac2ds
from datacube.ui.click import environment_option, pass_config
from odc.apps.dc_tools.utils import (
    allow_unsafe,
    archive_less_mature,
    fail_on_missing_lineage,
    index_update_dataset,
    update_if_exists_flag,
    publish_action,
    skip_lineage,
    statsd_setting,
    statsd_gauge_reporting,
    transform_stac,
    verify_lineage,
)
from sqlalchemy.exc import OperationalError, ProgrammingError

logging.basicConfig(
    level=logging.WARNING,
    format="%(asctime)s: %(levelname)s: %(message)s",
    datefmt="%m/%d/%Y %I:%M:%S",
)


@click.command("fs-to-dc")
@environment_option
@pass_config
@click.argument("input_directory", type=str, nargs=1)
@update_if_exists_flag
@allow_unsafe
@archive_less_mature
@transform_stac
@statsd_setting
@publish_action
@skip_lineage
@fail_on_missing_lineage
@verify_lineage
@click.option(
    "--glob",
    default=None,
    help="File system glob to use, defaults to **/*.yaml or **/*.json for STAC.",
)
def cli(
    cfg_env,
    input_directory,
    update_if_exists,
    allow_unsafe,
    stac,
    statsd_setting,
    glob,
    archive_less_mature,
    publish_action,
    skip_lineage,
    fail_on_missing_lineage,
    verify_lineage,
) -> None:
    try:
        dc = datacube.Datacube(env=cfg_env, app="fs-to-dc")
    except (OperationalError, ProgrammingError) as e:
        print(f"ERROR: {e}", file=sys.stderr)
        sys.exit(1)
    doc2ds = Doc2Dataset(
        dc.index,
        skip_lineage=skip_lineage,
        fail_on_missing_lineage=fail_on_missing_lineage,
        verify_lineage=verify_lineage,
    )

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
                    metadata = next(stac2ds([pystac.Item.from_dict(metadata)]))
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
    if statsd_setting:
        statsd_gauge_reporting(added, ["app:fs_to_dc", "action:added"], statsd_setting)
        statsd_gauge_reporting(
            failed, ["app:fs_to_dc", "action:failed"], statsd_setting
        )


if __name__ == "__main__":
    cli()
