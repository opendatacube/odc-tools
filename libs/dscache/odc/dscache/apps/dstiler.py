from functools import partial

import click
from odc import dscache
from odc.dscache.tools.tiling import (
    bin_by_native_tile,
    web_gs,
    extract_native_albers_tile,
    parse_gridspec,
)
from odc.dscache._dscache import mk_group_name
from odc.index import bin_dataset_stream


@click.command("dstiler")
@click.option("--native", is_flag=True, help="Use Landsat Path/Row as grouping")
@click.option(
    "--native-albers",
    is_flag=True,
    help="When datasets are in Albers (AU) grid already",
)
@click.option(
    "--web", type=int, help="Use web map tiling regime at supplied zoom level"
)
@click.option(
    "--grid",
    type=str,
    help="Grid spec or name 'crs;pixel_resolution;shape_in_pixels'|albers_au_25",
    default="albers_au_25",
)
@click.argument("dbfile", type=str, nargs=1)
def cli(native, native_albers, web, grid, dbfile):
    """Add spatial grouping to file db.

    Default grid is Australian Albers (EPSG:3577) with 100k by 100k tiles. But
    you can also group by Landsat path/row (--native), or Google's map tiling
    regime (--web zoom_level)

    \b
    Example for custom --grid:
      - rectangular: 'epsg:6933;-10x10;2000x3000'
                      ^crs      ^y  ^x ^ny  ^nx
      - square     : 'epsg:3857;10;10000'
      - named      : albers_au_25
                     albers_africa_10  (20,30,60 are also available)
    """
    cache = dscache.open_rw(dbfile)
    label = "Processing {} ({:,d} datasets)".format(dbfile, cache.count)
    group_prefix = "grid"
    gs = None

    cells = {}
    if native:
        group_prefix = "native"
        binner = partial(bin_by_native_tile, cells=cells)
    elif native_albers:
        group_prefix = "albers"
        binner = lambda dss: bin_by_native_tile(
            dss, cells, native_tile_id=extract_native_albers_tile
        )
    elif web is not None:
        gs = web_gs(web)
        group_prefix = "web_" + str(web)
        binner = lambda dss: bin_dataset_stream(gs, dss, cells)
    else:
        gs = parse_gridspec(grid)
        group_prefix = f"epsg{gs.crs.epsg:d}"
        binner = lambda dss: bin_dataset_stream(gs, dss, cells)

    if gs is not None:
        click.echo(f"Using gridspec: {gs}")
        cache.add_grid(gs, group_prefix)

    with click.progressbar(cache.get_all(), length=cache.count, label=label) as dss:
        for ds in binner(dss):
            pass

    click.echo("Total bins: {:d}".format(len(cells)))

    with click.progressbar(cells.values(), length=len(cells), label="Saving") as groups:
        for group in groups:
            cache.add_grid_tile(group_prefix, group.idx, group.dss)


if __name__ == "__main__":
    cli()
