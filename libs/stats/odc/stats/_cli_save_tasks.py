import click
import sys
from odc.io.text import click_range2d
from ._cli_common import main


@main.command("save-tasks")
@click.option(
    "--grid",
    type=str,
    help=(
        "Grid name or spec: albers_au_25,africa_{10|20|30|60},"
        "'crs;pixel_resolution;shape_in_pixels'"
    ),
    prompt="""Enter GridSpec
 one of albers_au_25, africa_{10|20|30|60}
 or custom like 'epsg:3857;30;5000' (30m pixels 5,000 per side in epsg:3857)
 >""",
    default=None,
)
@click.option(
    "--year",
    type=int,
    help="Only extract datasets for a given year. This is a shortcut for --temporal-range=<int>--P1Y",
)
@click.option(
    "--temporal_range",
    type=str,
    help="Only extract datasets for a given time range, Example '2020-05--P1M' month of May 2020",
)
@click.option(
    "--frequency", type=str, help="Specify temporal binning: annual|seasonal|all"
)
@click.option("--env", "-E", type=str, help="Datacube environment name")
@click.option(
    "-z",
    "complevel",
    type=int,
    default=6,
    help="Compression setting for zstandard 1-fast, 9+ good but slow",
)
@click.option(
    "--overwrite", is_flag=True, default=False, help="Overwrite output if it exists"
)
@click.option(
    "--tiles", help='Limit query to tiles example: "0:3,2:4"', callback=click_range2d
)
@click.option(
    "--debug",
    is_flag=True,
    default=False,
    hidden=True,
    help="Dump debug data to pickle",
)
@click.argument("product", type=str, nargs=1)
@click.argument("output", type=str, nargs=1, default="")
def save_tasks(
    grid,
    year,
    temporal_range,
    frequency,
    output,
    product,
    env,
    complevel,
    overwrite=False,
    tiles=None,
    debug=False,
):
    """
    Prepare tasks for processing (query db).

    <todo more help goes here>

    \b
    Not yet implemented features:
      - output product config
      - multi-product inputs

    """
    from datacube import Datacube
    from .tasks import SaveTasks
    from .model import DateTimeRange

    if temporal_range is not None and year is not None:
        print("Can only supply one of --year or --temporal_range", file=sys.stderr)
        sys.exit(1)

    if temporal_range is not None:
        try:
            temporal_range = DateTimeRange(temporal_range)
        except ValueError:
            print(f"Failed to parse supplied temporal_range: '{temporal_range}'")
            sys.exit(1)

    if year is not None:
        temporal_range = DateTimeRange.year(year)

    if frequency is not None:
        if frequency not in ("annual", "all", "seasonal"):
            print(f"Frequency must be one of annual|seasonal|all and not '{frequency}'")
            sys.exit(1)

    if output == "":
        if temporal_range is not None:
            output = f"{product}_{temporal_range.short}.db"
        else:
            output = f"{product}_all.db"

    try:
        tasks = SaveTasks(
            output, grid, frequency=frequency, overwrite=overwrite, complevel=complevel
        )
    except ValueError as e:
        print(str(e))
        sys.exit(1)

    def on_message(msg):
        print(msg)

    dc = Datacube(env=env)
    try:
        ok = tasks.save(
            dc,
            product,
            temporal_range=temporal_range,
            tiles=tiles,
            debug=debug,
            msg=on_message,
        )
    except ValueError as e:
        print(str(e))
        sys.exit(2)

    if not ok:
        # exit with error code, failure message was already printed
        sys.exit(3)
