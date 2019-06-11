import toolz
from datacube.model import Dataset
from . import _jsoncache as base


def ds2doc(ds):
    return (ds.id, dict(uris=ds.uris,
                        product=ds.type.name,
                        metadata=ds.metadata_doc))


def doc2ds(doc, products):
    if doc is None:
        return None

    p = products.get(doc['product'], None)
    if p is None:
        raise ValueError('No product named: %s' % doc['product'])
    return Dataset(p, doc['metadata'], uris=doc['uris'])


def build_dc_product_map(metadata_json, products_json):
    from datacube.model import metadata_from_doc, DatasetType

    mm = toolz.valmap(metadata_from_doc, metadata_json)

    def mk_product(doc, name):
        mt = doc.get('metadata_type')
        if mt is None:
            raise ValueError('Missing metadata_type key in product definition')
        metadata = mm.get(mt)

        if metadata is None:
            raise ValueError('No such metadata %s for product %s' % (mt, name))

        return DatasetType(metadata, doc)

    return mm, {k: mk_product(doc, k) for k, doc in products_json.items()}


def _metadata_from_products(products):
    mm = {}
    for p in products:
        m = p.metadata_type
        if m.name not in mm:
            mm[m.name] = m

    return mm


def train_dictionary(dss, dict_sz=8*1024):
    """ Given a finite sequence of Datasets train zstandard compression dictionary of a given size.

        Accepts both `Dataset` as well as "raw" datasets.

        Will return None if input sequence is empty.
    """
    docs = map(ds2doc, dss)
    return base.train_dictionary(docs, dict_sz=dict_sz)


class DatasetCache(object):
    """
    info:
       version: 4-bytes
       zdict: pre-trained compression dictionary, optional
       product/{name}: json
       metadata/{name}: json

    groups:
       Each group is named list of uuids

    udata:
       arbitrary user data (TODO)

    ds:
       uuid: compressed(json({product: str,
                              uris: [str],
                              metadata: object}))
    """
    def __init__(self, db: base.JsonBlobCache, products=None):
        """ Don't use this directly, use create_cache or open_(rw|ro).
        """

        if products is None:
            metadata, products = build_dc_product_map(db.get_info_dict('metadata/'),
                                                      db.get_info_dict('product/'))
        else:
            metadata = _metadata_from_products(products)

        self._db = db
        self._products = products
        self._metadata = metadata

    def close(self):
        """Write any outstanding product/metadata definitions (if in write mode) and
        close database file.

        """
        self._db.close()

    @property
    def readonly(self):
        return self._db.readonly

    @property
    def count(self):
        return self._db.count

    def put_group(self, name, uuids):
        """ Group is a named list of uuids
        """
        self._db.put_group(name, uuids)

    def get_group(self, name):
        """ Group is a named list of uuids
        """
        return self._db.get_group(name)

    def groups(self, raw=False, prefix=None):
        """Get list of tuples (group_name, group_size).

        :raw bool: Normally names are returned as strings, supplying raw=True
        would return bytes instead, this is needed if you are using group names
        that are not strings, like integers or tuples of basic types.

        :prefix str|bytes: Only report groups with name starting with prefix
        """
        return self._db.groups(raw=raw, prefix=prefix)

    @property
    def products(self):
        return self._products

    @property
    def metadata(self):
        return self._metadata

    def _add_metadata(self, metadata, transaction):
        self._metadata[metadata.name] = metadata
        self._db.append_info_dict('metadata/', {metadata.name: metadata.definition}, transaction)

    def _add_product(self, product, transaction):
        if product.metadata_type.name not in self._metadata:
            self._add_metadata(product.metadata_type, transaction)

        self._products[product.name] = product
        self._db.append_info_dict('product/', {product.name: product.definition}, transaction)

    def _ds2doc(self, ds):
        if ds.type.name not in self._products:
            self._add_product(ds.type, self._db.current_transaction)
        return ds2doc(ds)

    def bulk_save(self, dss):
        docs = (self._ds2doc(ds) for ds in dss)
        return self._db.bulk_save(docs)

    def tee(self, dss, max_transaction_size=10000):
        """Given a lazy stream of datasets persist them to disk and then pass through
        for further processing.
        :dss: stream of datasets
        :max_transaction_size int: How often to commit results to disk
        """
        docs = (self._ds2doc(ds) for ds in dss)
        return self._db.tee(docs, max_transaction_size=max_transaction_size)

    def get(self, uuid):
        """Extract single dataset with a given uuid, or return None if not found"""
        return doc2ds(self._db.get(uuid), self._products)

    def get_all(self):
        for _, v in self._db.get_all():
            yield doc2ds(v, self._products)

    def stream_group(self, group_name):
        for _, v in self._db.stream_group(group_name):
            yield doc2ds(v, self._products)


def open_ro(path,
            products=None,
            lock=False):
    """Open existing database in readonly mode.

    NOTE: default mode assumes db file is static (not being modified
    externally), if this is not the case, supply `lock=True` parameter.

    :path str: Path to the db could be folder or actual file

    :products: Override product dictionary with compatible products loaded from
    the datacube database, this is generally only needed if you intend to add
    datasets to the datacube index directly (i.e. without product matching
    metadata documents).

    :lock bool: Supply True if external process is changing DB concurrently.

    """

    db = base.open_ro(path, lock=lock)
    return DatasetCache(db)


def open_rw(path,
            products=None,
            max_db_sz=None,
            complevel=6):
    """Open existing database in append mode.

    :path str: Path to the db could be folder or actual file

    :products: Override product dictionary with compatible products loaded from
    the datacube database, this is generally only needed if you intend to add
    datasets to the datacube index directly (i.e. without product matching
    metadata documents).

    :max_db_sz int: Maximum size in bytes database file is allowed to grow to, defaults to 10Gb

    :complevel: Compression level (Zstandard) to use when storing datasets, 1
    fastest, 6 good and still fast, 20+ best but slower.
    """
    db = base.open_rw(path, max_db_sz=max_db_sz, complevel=complevel)
    return DatasetCache(db, products)


def create_cache(path,
                 complevel=6,
                 zdict=None,
                 max_db_sz=None,
                 truncate=False):
    """Create new file database or open existing one.

    :path str: Path where to create new database (this will be a directory with 2 files in it)

    :complevel int: Level of compressions to apply per dataset, bigger is slower but better compression.

    :zdict: Optional pre-trained compression dictionary

    :max_db_sz int: Maximum size in bytes (defaults to 10GiB)

    :truncate bool: If True wipe out any existing database and create new empty one.
    """
    db = base.create_cache(path,
                           complevel=complevel,
                           zdict=zdict,
                           max_db_sz=max_db_sz,
                           truncate=truncate)
    return DatasetCache(db)
