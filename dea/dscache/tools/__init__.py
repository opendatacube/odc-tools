"""
"""
import random
from .. import train_dictionary


def dictionary_from_product_list(dc,
                                 products,
                                 samples_per_product=10,
                                 dict_sz=8 * 1024):
    """Get a sample of datasets from a bunch of products and train compression
    dictionary.

    dc -- Datcube object
    products -- list of product names
    samples_per_product -- number of datasets per product to use for training
    dict_sz -- size of dictionary in bytes
    """

    if isinstance(products, str):
        products = [products]

    limit = samples_per_product * 10

    samples = []
    for p in products:
        dss = dc.find_datasets(product=p, limit=limit)
        random.shuffle(dss)
        samples.extend(dss[:samples_per_product])

    if len(samples) == 0:
        return None

    return train_dictionary(samples, dict_sz)


def db_connect(cfg=None):
    """ Create database connection from datacube config.

        cfg:
          None -- use default datacube config
          str  -- use config with a given name

          LocalConfig -- use loaded config object
    """
    from datacube.config import LocalConfig
    import psycopg2

    if isinstance(cfg, str) or cfg is None:
        cfg = LocalConfig.find(env=cfg)

    cfg_remap = dict(dbname='db_database',
                     user='db_username',
                     password='db_password',
                     host='db_hostname',
                     port='db_port')

    pg_cfg = {k: cfg.get(cfg_name, None)
              for k, cfg_name in cfg_remap.items()}

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
    from datacube.model import Dataset

    def raw2ds(ds):
        product = products.get(ds['product'], None)
        if product is None:
            raise ValueError('Missing product {}'.format(ds['product']))
        return Dataset(product, ds['metadata'], uris=ds['uris'])
    return raw2ds


def raw_dataset_stream(product, db, read_chunk=100, limit=None):
    """ Given a product name stream all "active" datasets from db that belong to that product.

    Datasets are returned in "raw form", basically just a python dictionary with fields:

    - product: str -- product name
    - uris: [str] -- list of dataset uris
    - metadata: dict -- dataset metadata document
    """

    assert isinstance(limit, (int, type(None)))

    if isinstance(db, str) or db is None:
        db = db_connect(db)

    query = '''
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
'''.format(limit='LIMIT {:d}'.format(limit) if limit else '')

    cur = db.cursor(name='c{:04X}'.format(random.randint(0, 0xFFFF)))
    cur.execute(query, dict(product=product))

    while True:
        chunk = cur.fetchmany(read_chunk)
        if not chunk:
            break

        for (ds,) in chunk:
            yield ds

    cur.close()


class DcTileExtract(object):
    """ Construct ``datacube.api.grid_workflow.Tile`` object from dataset cache.
    """

    def __init__(self, cache,
                 group_by='time',
                 key_fmt=None,
                 grid_spec=None):
        from datacube.api.query import query_group_by
        from .dstiler import GS_ALBERS

        self._cache = cache
        self._grouper = query_group_by(group_by=group_by)
        self._grid_spec = GS_ALBERS if grid_spec is None else grid_spec
        self._key_fmt = 'albers/{:+03d}{:+03d}' if key_fmt is None else key_fmt

    def __call__(self, tile_idx, _y=None):
        from datacube import Datacube
        from datacube.api.grid_workflow import Tile

        if _y is not None:
            tile_idx = (tile_idx, _y)

        k = self._key_fmt.format(*tile_idx)
        dss = list(self._cache.stream_group(k))

        geobox = self._grid_spec.tile_geobox(tile_idx)
        sources = Datacube.group_datasets(dss, self._grouper)
        return Tile(sources, geobox)
