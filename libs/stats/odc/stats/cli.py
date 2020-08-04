import click
from tqdm.auto import tqdm
import sys


@click.group(help="Stats command line interface")
def main():
    pass


@main.command('save-tasks')
@click.option('--grid',
              type=str,
              help=("Grid name or spec: albers_au_25,albers_africa_{10|20|30|60},"
                    "'crs;pixel_resolution;shape_in_pixels'"),
              prompt="""Enter GridSpec
 one of albers_au_25, albers_africa_{10|20|30|60}
 or custom like 'epsg:3857;30;5000' (30m pixels 5,000 per side in epsg:3857)
 >""",
              default=None)
@click.option('--year',
              type=int,
              prompt="Enter year",
              help="Only extract datasets for a given year")
@click.option('--env', '-E', type=str, help='Datacube environment name')
@click.option('-z', 'complevel',
              type=int,
              default=6,
              help='Compression setting for zstandard 1-fast, 9+ good but slow')
@click.option('--overwrite',
              is_flag=True,
              default=False,
              help='Overwrite output if it exists')
@click.argument('product', type=str, nargs=1)
@click.argument('output', type=str, nargs=1, default='')
def save_tasks(grid, year, output, product, env, complevel, overwrite=False):
    """
    Prepare tasks for processing (query db)

    <todo more help goes here>

    \b
    Not yet implemented features:
      - output product config
      - multi-product inputs

    """
    from odc.index import chopped_dss, bin_dataset_stream, dataset_count
    from odc.dscache import create_cache, db_exists
    from odc.dscache.tools import dictionary_from_product_list
    from odc.dscache.tools.tiling import parse_gridspec_with_name
    from odc.dscache.tools.profiling import ds_stream_test_func
    from datacube import Datacube

    time_period = f'{year}'

    if output == '':
        output = f'{product}_{year}.db'

    if db_exists(output) and overwrite is False:
        print(f"File database already exists: {output}, use --overwrite flag to force deletion", file=sys.stderr)
        sys.exit(1)

    try:
        grid, gridspec = parse_gridspec_with_name(grid)
    except ValueError:
        print(f"""Failed to recognize/parse gridspec: '{grid}'
  Try one of the named ones: albers_au_25, albers_africa_{10|20|30|60}
  or define custom 'crs:3857;30;5000' - 30m pixels 5,000 pixels per side""", file=sys.stderr)
        sys.exit(2)

    cfg = dict(
        freq='1Y',
        year=year,
        grid=grid,
    )

    print(f"Will write to {output}")
    dc = Datacube(env=env)

    print("Connecting to the database, counting datasets")
    n_dss = dataset_count(dc.index, product=product, time=time_period)
    print(f"Processing {n_dss:,d} datasets for the year {year}")

    print("Training compression dictionary")
    zdict = dictionary_from_product_list(dc, [product], samples_per_product=100)
    print(".. done")

    cache = create_cache(output, zdict=zdict, complevel=complevel, truncate=overwrite)
    cache.add_grid(gridspec, grid)
    cache.append_info_dict("stats/", dict(config=cfg))

    cells = {}
    dss = chopped_dss(dc, product=product, time=time_period, freq='w')
    dss = cache.tee(dss)
    dss = bin_dataset_stream(gridspec, dss, cells, persist=lambda ds: (ds.id, ds.center_time))
    dss = tqdm(dss, total=n_dss)

    rr = ds_stream_test_func(dss)
    print(rr.text)

    n_tiles = len(cells)
    print(f"Total of {n_tiles:,d} output tiles")

    temporal_k = (f'{cfg["year"]}--{cfg["freq"]}',)
    print("Saving spatial index to disk")
    cache.add_grid_tiles(grid, {temporal_k + k: [d[0] for d in x.dss]
                                for k, x in cells.items()})
    print(".. done")
