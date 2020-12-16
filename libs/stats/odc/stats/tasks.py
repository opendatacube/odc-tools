from typing import Optional, Tuple, Union, Callable, Any, Dict, List, Iterable, Iterator
from types import SimpleNamespace
from collections import namedtuple
from datetime import datetime
import pickle
import json
import os
import boto3
import botocore
from pathlib import Path
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
from odc.aws import s3_download

from .model import DateTimeRange, Task, OutputProduct, TileIdx, TileIdx_txy, TileIdx_xy
from ._gjson import gs_bounds, compute_grid_info, gjson_from_tasks
from .utils import bin_annual, bin_full_history, bin_generic, bin_seasonal

TilesRange2d = Tuple[Tuple[int, int], Tuple[int, int]]
CompressedDataset = namedtuple("CompressedDataset", ["id", "time"])


def _xy(tidx: TileIdx) -> TileIdx_xy:
    return tidx[-2:]


def compress_ds(ds: Dataset) -> CompressedDataset:
    dt = normalise_dt(ds.center_time)
    return CompressedDataset(ds.id, dt)


def is_tile_in(tidx: Tuple[int, int], tiles: TilesRange2d) -> bool:
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
    def __init__(
        self,
        output: str,
        grid: str,
        frequency: str = "annual",
        overwrite: bool = False,
        complevel: int = 6,
    ):

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

    def save(
        self,
        dc: Datacube,
        product: str,
        temporal_range: Union[str, DateTimeRange, None] = None,
        tiles: Optional[TilesRange2d] = None,
        msg: Optional[Callable[[str], Any]] = None,
        debug: bool = False,
    ) -> bool:

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
            cfg["tiles"] = tiles
            query["geopolygon"] = gs_bounds(self._gridspec, tiles)

        # TODO: properly handle UTC offset when limiting query to a given time temporal_range
        #       Basically need to pad query by 12hours, then trim datasets post-query
        if temporal_range is not None:
            query.update(
                temporal_range.dc_query(pad=0.6)
            )  # pad a bit more than half a day on each side
            cfg["temporal_range"] = temporal_range.short

        cfg["query"] = sanitize_query(query)

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

        cache = DatasetCache.create(
            self._output,
            zdict=zdict,
            complevel=self._complevel,
            truncate=self._overwrite,
        )
        cache.add_grid(self._gridspec, self._grid)
        cache.append_info_dict("stats/", dict(config=cfg))

        cells: Dict[Tuple[int, int], Any] = {}
        if "time" in query:
            dss = chopped_dss(dc, freq="w", **query)
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
            cells = {
                tidx: dss for tidx, dss in cells.items() if is_tile_in(tidx, tiles)
            }

        n_tiles = len(cells)
        msg(f"Total of {n_tiles:,d} spatial tiles")

        if self._frequency == "all":
            tasks = bin_full_history(cells, start=dt_range.start, end=dt_range.end)
        elif self._frequency == "seasonal":
            tasks = bin_seasonal(cells, months=3, anchor=12)
        elif temporal_range is not None:
            tasks = bin_generic(cells, [temporal_range])
        else:
            tasks = bin_annual(cells)

        tasks_uuid = {k: [ds.id for ds in dss] for k, dss in tasks.items()}

        msg(f"Saving tasks to disk ({len(tasks)})")
        cache.add_grid_tiles(self._grid, tasks_uuid)
        msg(".. done")

        csv_path = self.out_path(".csv")
        msg(f"Writing summary to {csv_path}")
        with open(csv_path, "wt") as f:
            f.write('"T","X","Y","datasets","days"\n')

            for p, x, y in sorted(tasks):
                dss = tasks[(p, x, y)]
                n_dss = len(dss)
                n_days = len(set(ds.time.date() for ds in dss))
                line = f'"{p}", {x:+05d}, {y:+05d}, {n_dss:4d}, {n_days:4d}\n'
                f.write(line)

        msg("Dumping GeoJSON(s)")
        grid_info = compute_grid_info(
            cells, resolution=max(self._gridspec.tile_size) / 4
        )
        tasks_geo = gjson_from_tasks(tasks, grid_info)
        for temporal_range, gjson in tasks_geo.items():
            fname = self.out_path(f"-{temporal_range}.geojson")
            msg(f"..writing to {fname}")
            with open(fname, "wt") as f:
                json.dump(gjson, f)

        if debug:
            pkl_path = self.out_path("-cells.pkl")
            msg(f"Saving debug info to: {pkl_path}")
            with open(pkl_path, "wb") as fb:
                pickle.dump(cells, fb)

            pkl_path = self.out_path("-tasks.pkl")
            msg(f"Saving debug info to: {pkl_path}")
            with open(pkl_path, "wb") as fb:
                pickle.dump(tasks, fb)

        return True


class TaskReader:
    def __init__(
        self, cache: Union[str, DatasetCache], product: Optional[OutputProduct] = None
    ):
        self._cache_path = None
        if isinstance(cache, str):
            if cache.startswith("s3"):
                self._cache_path = s3_download(cache)
                cache = self._cache_path
            cache = DatasetCache.open_ro(cache)

        # TODO: verify this things are set in the file
        cfg = cache.get_info_dict("stats/config")
        grid = cfg["grid"]
        gridspec = cache.grids[grid]

        self._product = product
        self._dscache = cache
        self._cfg = cfg
        self._grid = grid
        self._gridspec = gridspec
        self._all_tiles = sorted(idx for idx, _ in cache.tiles(grid))

    def __del__(self):
        if self._cache_path is not None:
            os.unlink(self._cache_path)

    def __repr__(self) -> str:
        grid, path, n = self._grid, str(self._dscache.path), len(self._all_tiles)
        return f"<{path}> grid:{grid} n:{n:,d}"

    def _resolve_product(self, product: Optional[OutputProduct]) -> OutputProduct:
        if product is None:
            product = self._product

        if product is None:
            raise ValueError("Product is not supplied and default is not set")
        return product

    @property
    def all_tiles(self) -> List[TileIdx_txy]:
        return self._all_tiles

    def datasets(self, tile_index: TileIdx_txy) -> Tuple[Dataset, ...]:
        return tuple(
            ds for ds in self._dscache.stream_grid_tile(tile_index, self._grid)
        )

    def load_task(
        self, tile_index: TileIdx_txy, product: Optional[OutputProduct] = None
    ) -> Task:
        product = self._resolve_product(product)

        dss = self.datasets(tile_index)
        tidx_xy = _xy(tile_index)

        return Task(
            product=product,
            tile_index=tidx_xy,
            geobox=self._gridspec.tile_geobox(tidx_xy),
            time_range=DateTimeRange(tile_index[0]),
            datasets=dss,
        )

    def stream(
        self, tiles: Iterable[TileIdx_txy], product: Optional[OutputProduct] = None
    ) -> Iterator[Task]:
        product = self._resolve_product(product)
        for tidx in tiles:
            yield self.load_task(tidx, product)
