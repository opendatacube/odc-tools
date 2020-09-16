from typing import Optional, Tuple, Union, Callable, Any, Dict
from types import SimpleNamespace
from collections import namedtuple
from datetime import datetime
import pickle
import json
from tqdm.auto import tqdm

from odc.dscache import DatasetCache
from datacube import Datacube
from datacube.model import Dataset
from datacube.utils.geometry import Geometry
from datacube.utils.documents import transform_object_tree
from datacube.utils.dates import normalise_dt

from odc.index import chopped_dss, bin_dataset_stream, dataset_count, all_datasets
from odc.dscache.tools import dictionary_from_product_list
from odc.dscache.tools.tiling import parse_gridspec_with_name
from odc.dscache.tools.profiling import ds_stream_test_func

from .model import DateTimeRange
from .metadata import gs_bounds, compute_grid_info, gjson_from_tasks
from .utils import bin_annual, bin_full_history, bin_generic, bin_seasonal

TilesRange2d = Tuple[Tuple[int, int], Tuple[int, int]]
CompressedDataset = namedtuple("CompressedDataset", ['id', 'time'])


def compress_ds(ds: Dataset) -> CompressedDataset:
    dt = normalise_dt(ds.center_time)
    return CompressedDataset(ds.id, dt)


def is_tile_in(tidx: Tuple[int, int],
               tiles: TilesRange2d) -> bool:
    (x0, x1), (y0, y1) = tiles
    x, y = tidx
    return (x0 <= x < x1) and (y0 <= y < y1)


def out_path(suffix: str, base: str) -> str:
    if base.endswith(".db"):
        base = base[:-3]
    return base + suffix


def sanitize_query(query):
    def sanitize(v):
        if isinstance(v, Geometry):
            return v.json
        if isinstance(v, datetime):
            return v.isoformat()
        return v
    return transform_object_tree(sanitize, query)


class SaveTasks:
    def __init__(self,
                 output: str,
                 grid: str,
                 frequency: str = 'annual',
                 overwrite: bool = False,
                 complevel: int = 6):

        if DatasetCache.exists(output) and overwrite is False:
            raise ValueError(f"File database already exists: {output}")

        grid, gridspec = parse_gridspec_with_name(grid)

        self._output = output
        self._overwrite = overwrite
        self._complevel = complevel
        self._grid = grid
        self._gridspec = gridspec
        self._frequency = frequency

    def out_path(self, suffix: str) -> str:
        return out_path(suffix, self._output)

    def save(self,
             dc: Datacube,
             product: str,
             temporal_range: Union[str, DateTimeRange, None] = None,
             tiles: Optional[TilesRange2d] = None,
             msg: Optional[Callable[[str], Any]] = None,
             debug: bool = False) -> bool:

        dt_range = SimpleNamespace(start=None, end=None)

        def _update_start_end(x, out):
            if out.start is None:
                out.start = x
                out.end = x
            else:
                out.start = min(out.start, x)
                out.end = max(out.end, x)

        def persist(ds: Dataset) -> CompressedDataset:
            _ds = compress_ds(ds)
            _update_start_end(_ds.time, dt_range)
            return _ds

        def msg_default(msg):
            pass

        if msg is None:
            msg = msg_default

        if isinstance(temporal_range, str):
            temporal_range = DateTimeRange(temporal_range)

        cfg: Dict[str, Any] = dict(
            grid=self._grid,
            freq=self._frequency,
        )

        query = dict(product=product)

        if tiles is not None:
            (x0, x1), (y0, y1) = tiles
            msg(f"Limit search to tiles: x:[{x0}, {x1}) y:[{y0}, {y1})")
            cfg['tiles'] = tiles
            query['geopolygon'] = gs_bounds(self._gridspec, tiles)

        # TODO: properly handle UTC offset when limiting query to a given time temporal_range
        #       Basically need to pad query by 12hours, then trim datasets post-query
        if temporal_range is not None:
            query.update(temporal_range.dc_query())
            cfg['temporal_range'] = temporal_range.short

        cfg['query'] = sanitize_query(query)

        if DatasetCache.exists(self._output) and self._overwrite is False:
            raise ValueError(f"File database already exists: {self._output}")

        msg("Connecting to the database, counting datasets")
        n_dss = dataset_count(dc.index, **query)
        if n_dss == 0:
            msg("Found no datasets to process")
            return False

        msg(f"Processing {n_dss:,d} datasets")

        msg("Training compression dictionary")
        zdict = dictionary_from_product_list(dc, [product], samples_per_product=100)
        msg(".. done")

        cache = DatasetCache.create(self._output, zdict=zdict,
                                    complevel=self._complevel,
                                    truncate=self._overwrite)
        cache.add_grid(self._gridspec, self._grid)
        cache.append_info_dict("stats/", dict(config=cfg))

        cells: Dict[Tuple[int, int], Any] = {}
        if 'time' in query:
            dss = chopped_dss(dc, freq='w', **query)
        else:
            if len(query) == 1:
                dss = all_datasets(dc, **query)
            else:
                # note: this blocks for large result sets
                dss = dc.find_datasets_lazy(**query)

        dss = cache.tee(dss)
        dss = bin_dataset_stream(self._gridspec, dss, cells, persist=persist)
        dss = tqdm(dss, total=n_dss)

        rr = ds_stream_test_func(dss)
        msg(rr.text)

        if tiles is not None:
            # prune out tiles that were not requested
            cells = {tidx: dss
                     for tidx, dss in cells.items()
                     if is_tile_in(tidx, tiles)}

        n_tiles = len(cells)
        msg(f"Total of {n_tiles:,d} spatial tiles")

        if self._frequency == 'all':
            tasks = bin_full_history(cells,
                                     start=dt_range.start,
                                     end=dt_range.end)
        elif self._frequency == 'seasonal':
            tasks = bin_seasonal(cells,
                                 months=3,
                                 anchor=12)
        elif temporal_range is not None:
            tasks = bin_generic(cells, [temporal_range])
        else:
            tasks = bin_annual(cells)

        tasks_uuid = {k: [ds.id for ds in dss]
                      for k, dss in tasks.items()}

        msg(f"Saving tasks to disk ({len(tasks)})")
        cache.add_grid_tiles(self._grid, tasks_uuid)
        msg(".. done")

        csv_path = self.out_path(".csv")
        msg(f"Writing summary to {csv_path}")
        with open(csv_path, 'wt') as f:
            f.write('"T","X","Y","datasets","days"\n')

            for p, x, y in sorted(tasks):
                dss = tasks[(p, x, y)]
                n_dss = len(dss)
                n_days = len(set(ds.time.date() for ds in dss))
                line = f'"{p}", {x:+05d}, {y:+05d}, {n_dss:4d}, {n_days:4d}\n'
                f.write(line)

        msg("Dumping GeoJSON(s)")
        grid_info = compute_grid_info(cells,
                                      resolution=max(self._gridspec.tile_size)/4)
        tasks_geo = gjson_from_tasks(tasks, grid_info)
        for temporal_range, gjson in tasks_geo.items():
            fname = self.out_path(f'-{temporal_range}.geojson')
            msg(f"..writing to {fname}")
            with open(fname, 'wt') as f:
                json.dump(gjson, f)

        if debug:
            pkl_path = self.out_path('-cells.pkl')
            msg(f"Saving debug info to: {pkl_path}")
            with open(pkl_path, "wb") as fb:
                pickle.dump(cells, fb)

            pkl_path = self.out_path('-tasks.pkl')
            msg(f"Saving debug info to: {pkl_path}")
            with open(pkl_path, "wb") as fb:
                pickle.dump(tasks, fb)

        return True
