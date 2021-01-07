"""
"""
from typing import Dict, Any, Optional
import random
from datacube import Datacube
from datacube.model import Dataset
from datacube.api.grid_workflow import Tile

from odc.index import group_by_nothing
from .. import DatasetCache


def dictionary_from_product_list(
    dc, products, samples_per_product=10, dict_sz=8 * 1024, query=None
):

    """Get a sample of datasets from a bunch of products and train compression
    dictionary.

    dc -- Datcube object
    products -- list of product names
    samples_per_product -- number of datasets per product to use for training
    dict_sz -- size of dictionary in bytes
    """

    if isinstance(products, str):
        products = [products]

    if query is None:
        query = {}

    limit = samples_per_product * 10

    samples = []
    for p in products:
        dss = dc.find_datasets(product=p, limit=limit, **query)
        random.shuffle(dss)
        samples.extend(dss[:samples_per_product])

    if len(samples) == 0:
        return None

    return DatasetCache.train_dictionary(samples, dict_sz)


def db_connect(cfg=None):
    """Create database connection from datacube config.

    cfg:
      None -- use default datacube config
      str  -- use config with a given name

      LocalConfig -- use loaded config object
    """
    from datacube.config import LocalConfig
    import psycopg2

    if isinstance(cfg, str) or cfg is None:
        cfg = LocalConfig.find(env=cfg)

    cfg_remap = dict(
        dbname="db_database",
        user="db_username",
        password="db_password",
        host="db_hostname",
        port="db_port",
    )

    pg_cfg = {k: cfg.get(cfg_name, None) for k, cfg_name in cfg_remap.items()}

    return psycopg2.connect(**pg_cfg)


def mk_raw2ds(products):
    """Convert "raw" dataset to `datacube.model.Dataset`.

    products -- dictionary from product name to `Product` object (`DatasetType`)

    returns: function that maps: `dict` -> `datacube.model.Dataset`

    This function can raise `ValueError` if dataset product is not found in the
    supplied products dictionary.


    Here "raw dataset" is just a python dictionary with fields:

    - product: str -- product name
    - uris: [str] -- list of dataset uris
    - metadata: dict -- dataset metadata document

    see `raw_dataset_stream`

    """

    def raw2ds(ds):
        product = products.get(ds["product"], None)
        if product is None:
            raise ValueError("Missing product {}".format(ds["product"]))
        return Dataset(product, ds["metadata"], uris=ds["uris"])

    return raw2ds


def raw_dataset_stream(product, db, read_chunk=100, limit=None):
    """Given a product name stream all "active" datasets from db that belong to that product.

    Datasets are returned in "raw form", basically just a python dictionary with fields:

    - product: str -- product name
    - uris: [str] -- list of dataset uris
    - metadata: dict -- dataset metadata document
    """

    assert isinstance(limit, (int, type(None)))

    if isinstance(db, str) or db is None:
        db = db_connect(db)

    query = """
select
jsonb_build_object(
  'product', %(product)s,
  'uris', array((select _loc_.uri_scheme ||':'||_loc_.uri_body
                 from agdc.dataset_location as _loc_
                 where _loc_.dataset_ref = agdc.dataset.id and _loc_.archived is null
                 order by _loc_.added desc, _loc_.id desc)),
  'metadata', metadata) as dataset
from agdc.dataset
where archived is null
and dataset_type_ref = (select id from agdc.dataset_type where name = %(product)s)
{limit};
""".format(
        limit="LIMIT {:d}".format(limit) if limit else ""
    )

    cur = db.cursor(name="c{:04X}".format(random.randint(0, 0xFFFF)))
    cur.execute(query, dict(product=product))

    while True:
        chunk = cur.fetchmany(read_chunk)
        if not chunk:
            break

        for (ds,) in chunk:
            yield ds

    cur.close()


def gs_albers():
    from datacube.model import GridSpec
    import datacube.utils.geometry as geom

    return GridSpec(
        crs=geom.CRS("EPSG:3577"), tile_size=(100000.0, 100000.0), resolution=(-25, 25)
    )


class DcTileExtract(object):
    """Construct ``datacube.api.grid_workflow.Tile`` object from dataset cache."""

    def __init__(self, cache, grid=None, group_by="time"):

        gs = cache.grids.get(grid, None)
        if gs is None:
            raise ValueError(f"No such grid: ${grid}")

        self._cache = cache
        self._grid = grid
        self._gs = gs
        self._default_groupby = group_by

    def __call__(self, tile_idx, _y=None, group_by=None):
        if _y is not None:
            tile_idx = (tile_idx, _y)

        if group_by is None:
            group_by = self._default_groupby

        dss = list(self._cache.stream_grid_tile(tile_idx, grid=self._grid))
        if group_by == "nothing":
            sources = group_by_nothing(dss)
        else:
            sources = Datacube.group_datasets(dss, group_by)

        geobox = self._gs.tile_geobox(tile_idx)
        return Tile(sources, geobox)


def grid_tiles_to_geojson(
    cache: DatasetCache,
    grid: str,
    style: Optional[Dict[str, Any]] = None,
    wrapdateline: bool = False,
) -> Dict[str, Any]:
    """
    Render tiles of a given grid to GeoJSON.

    each tile is a GeoJSON Feature with following properties:

     .title   -- str: grid index as a string
     .count   -- int: count of datasets overlapping with this grid tile

    :param cache: Dataset cache from which to read gridspec and tiles
    :param grid: Name of the grid to dump
    :param style: Optional style dictionary (will be included in every tile)
    :param wrapdateline: If set check for lon=180 intersect and do "the right thing" (slower)
    """
    if style is None:
        # these are understood by github renderer
        style = {"fill-opacity": 0, "stroke-width": 0.5}

    gs = cache.grids.get(grid, None)
    if gs is None:
        raise ValueError(f"No such grid: {grid}")

    resolution = abs(gs.tile_size[0]) / 4  # up to 4 points per side

    features = [
        dict(
            type="Feature",
            geometry=gs.tile_geobox(tidx)
            .extent.to_crs(
                "epsg:4326", resolution=resolution, wrapdateline=wrapdateline
            )
            .json,
            properties={
                "title": f"{tidx[0]:+05d},{tidx[1]:+05d}",
                "count": cc,
                **style,
            },
        )
        for tidx, cc in cache.tiles(grid)
    ]

    return {"type": "FeatureCollection", "features": features}
