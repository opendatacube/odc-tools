import click
from tqdm.auto import tqdm
import sys
import pickle
from collections import namedtuple
from odc.io.text import click_range2d

CompressedDataset = namedtuple("CompressedDataset", ['id', 'time'])


def is_tile_in(tidx, tiles):
    (x0, x1), (y0, y1) = tiles
    x, y = tidx
    return (x0 <= x < x1) and (y0 <= y < y1)


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
@click.option('--tiles',
              help='Limit query to tiles example: "0:3,2:4"',
              callback=click_range2d)
@click.option('--debug',
              is_flag=True,
              default=False,
              hidden=True,
              help='Dump debug data to pickle')
@click.argument('product', type=str, nargs=1)
@click.argument('output', type=str, nargs=1, default='')
def save_tasks(grid, year, output, product, env, complevel,
               overwrite=False,
               tiles=None,
               debug=False):
    """
    Prepare tasks for processing (query db).

    <todo more help goes here>

    \b
    Not yet implemented features:
      - output product config
      - multi-product inputs

    """
    import toolz
    from odc.index import chopped_dss, bin_dataset_stream, dataset_count, all_datasets
    from odc.dscache import create_cache, db_exists
    from odc.dscache.tools import dictionary_from_product_list
    from odc.dscache.tools.tiling import parse_gridspec_with_name
    from odc.dscache.tools.profiling import ds_stream_test_func
    from datacube import Datacube
    from datacube.model import Dataset
    from datacube.utils.geometry import Geometry
    from .metadata import gs_bounds

    if output == '':
        if year is not None:
            output = f'{product}_{year}.db'
        else:
            output = f'{product}_all.db'

    def compress_ds(ds: Dataset) -> CompressedDataset:
        return CompressedDataset(ds.id, ds.center_time)

    def out_path(suffix, base=output):
        return base.rstrip(".db") + suffix

    def sanitize_query(query):
        def sanitize(v):
            if isinstance(v, Geometry):
                return v.json
            return v
        return {k: sanitize(v) for k, v in query.items()}

    try:
        grid, gridspec = parse_gridspec_with_name(grid)
    except ValueError:
        print(f"""Failed to recognize/parse gridspec: '{grid}'
  Try one of the named ones: albers_au_25, albers_africa_{10|20|30|60}
  or define custom 'crs:3857;30;5000' - 30m pixels 5,000 pixels per side""", file=sys.stderr)
        sys.exit(2)

    cfg = dict(
        grid=grid,
        freq='1Y',
    )

    query = dict(product=product)

    if tiles is not None:
        (x0, x1), (y0, y1) = tiles
        print(f"Limit search to tiles: x:[{x0}, {x1}) y:[{y0}, {y1})")
        cfg['tiles'] = tiles
        query['geopolygon'] = gs_bounds(gridspec, tiles)

    if year is not None:
        cfg['year'] = year
        query['time'] = f'{year}'

    if db_exists(output) and overwrite is False:
        print(f"File database already exists: {output}, use --overwrite flag to force deletion", file=sys.stderr)
        sys.exit(1)

    print(f"Will write to {output}")

    cfg['query'] = sanitize_query(query)

    dc = Datacube(env=env)

    print("Connecting to the database, counting datasets")
    n_dss = dataset_count(dc.index, **query)
    if n_dss == 0:
        print("Found no datasets to process")
        sys.exit(3)

    print(f"Processing {n_dss:,d} datasets")

    print("Training compression dictionary")
    zdict = dictionary_from_product_list(dc, [product], samples_per_product=100)
    print(".. done")

    cache = create_cache(output, zdict=zdict, complevel=complevel, truncate=overwrite)
    cache.add_grid(gridspec, grid)

    cache.append_info_dict("stats/", dict(config=cfg))

    cells = {}
    if 'time' in query:
        dss = chopped_dss(dc, freq='w', **query)
    else:
        if len(query) == 1:
            dss = all_datasets(dc, **query)
        else:
            # note: this blocks for large result sets
            dss = dc.find_datasets_lazy(**query)

    dss = cache.tee(dss)
    dss = bin_dataset_stream(gridspec, dss, cells, persist=compress_ds)
    dss = tqdm(dss, total=n_dss)

    rr = ds_stream_test_func(dss)
    print(rr.text)

    if tiles is not None:
        # prune out tiles that were not requested
        cells = {tidx: dss
                 for tidx, dss in cells.items()
                 if is_tile_in(tidx, tiles)}

    n_tiles = len(cells)
    print(f"Total of {n_tiles:,d} spatial tiles")

    if year is not None:
        temporal_k = (f'{year}--P1Y',)
        tasks = {temporal_k + k: x.dss
                 for k, x in cells.items()}
    else:
        tasks = {}
        for tidx, cell in cells.items():
            grouped = toolz.groupby(lambda ds: ds.time.year, cell.dss)
            for year, dss in grouped.items():
                temporal_k = (f"{year}--P1Y",)
                tasks[temporal_k + tidx] = dss

    tasks_uuid = {k: [ds.id for ds in dss]
                  for k, dss in tasks.items()}

    print(f"Saving tasks to disk ({len(tasks)})")
    cache.add_grid_tiles(grid, tasks_uuid)
    print(".. done")

    csv_path = out_path(".csv")
    print(f"Writing summary to {csv_path}")
    with open(csv_path, 'wt') as f:
        f.write('"Period", "X", "Y", "datasets", "days"\n')

        for p, x, y in sorted(tasks):
            dss = tasks[(p, x, y)]
            n_dss = len(dss)
            n_days = len(set(ds.time.date() for ds in dss))
            line = f'"{p}", {x:+05d}, {y:+05d}, {n_dss:4d}, {n_days:4d}\n'
            f.write(line)

    if debug:
        pkl_path = out_path('-cells.pkl')
        print(f"Saving debug info to: {pkl_path}")
        with open(pkl_path, "wb") as f:
            pickle.dump(cells, f)

        pkl_path = out_path('-tasks.pkl')
        print(f"Saving debug info to: {pkl_path}")
        with open(pkl_path, "wb") as f:
            pickle.dump(tasks, f)
