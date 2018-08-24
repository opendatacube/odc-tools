from uuid import UUID
import json
import lmdb
import zstandard
import operator
import functools
import itertools
import toolz
from types import SimpleNamespace
from pathlib import Path
from datacube.model import Dataset

FORMAT_VERSION = b'0001'


def key_to_bytes(k):
    if isinstance(k, str):
        return k.encode('utf8')
    if isinstance(k, UUID):
        return k.bytes
    if isinstance(k, bytes):
        return k
    if isinstance(k, int):
        if k.bit_length() < 32:
            return k.to_bytes(4, 'big')
        elif k.bit_length() < 128:
            return k.to_bytes(16, 'big')
        else:
            return str(k).decode('utf8')
    if isinstance(k, tuple):
        return functools.reduce(operator.add, map(key_to_bytes, k))

    raise ValueError('Key must be one of str|bytes|int|UUID|tuple')


def dict2jsonKV(oo, prefix=None, compressor=None):
    for k, doc in oo.items():
        data = json.dumps(doc, separators=(',', ':')).encode('utf8')
        if compressor is not None:
            data = compressor.compress(data)
        if prefix is not None:
            k = prefix + k

        yield (key_to_bytes(k), data)


def ds2bytes(ds):
    k = key_to_bytes(ds.id)

    doc = dict(uris=ds.uris,
               product=ds.type.name,
               metadata=ds.metadata_doc)

    d = json.dumps(doc, separators=(',', ':')).encode('utf8')
    return (k, d)


def doc2bytes(raw_ds):
    ''' raw_ds is

        metadata:
          id: <uuid: string>
          * other fields*
        uris: [<uri:string>]
        product: <string>
    '''
    k = UUID(toolz.get_in(['metadata', 'id'], raw_ds)).bytes
    d = json.dumps(raw_ds, separators=(',', ':')).encode('utf8')
    return (k, d)


def doc2ds(doc, products):
    p = products.get(doc['product'], None)
    if p is None:
        raise ValueError('No product named: %s' % doc['product'])
    return Dataset(p, doc['metadata'], uris=doc['uris'])


def save_products(products, transaction, compressor, overwrite=False):
    def get_metadata_definitions(products):
        mm = {}
        for p in products:
            m = p.metadata_type
            if m.name not in mm:
                mm[m.name] = m.definition

        return mm

    mm = get_metadata_definitions(products.values())
    products = toolz.valmap(lambda p: p.definition, products)

    for k, d in itertools.chain(dict2jsonKV(mm, 'metadata/', compressor),
                                dict2jsonKV(products, 'product/', compressor)):
        transaction.put(k, d, overwrite=overwrite, dupdata=False)


def train_dictionary(dss, dict_sz=8*1024):
    def to_bytes(o):
        if isinstance(o, dict):
            _, d = doc2bytes(o)
        else:
            _, d = ds2bytes(o)
        return d

    sample = list(map(to_bytes, dss))
    return zstandard.train_dictionary(dict_sz, sample).as_bytes()


class DatasetCache(object):
    """
    info:
       version: 4-bytes
       zdict: pre-trained compression dictionary, optional
       product/{name}: json
       metadata/{name}: json

    udata:
       arbitrary user data (TODO)

    ds:
       uuid: compressed(json({product: str,
                              uris: [str],
                              metadata: object}))
    """
    def __init__(self, state):
        """ Don't use this directly, use create_cache or open_cache.
        """

        self._dbs = state.dbs
        self._comp = state.comp
        self._decomp = state.decomp
        self._products = state.products

    def _store_products(self):
        with self._dbs.main.begin(self._dbs.info, write=True) as tr:
            save_products(self._products, tr, self._comp)

    def sync(self):
        if not self.readonly:
            self._store_products()

    def __del__(self):
        self.sync()

    @property
    def readonly(self):
        return self._comp is None

    @property
    def products(self):
        return self._products

    def _ds2kv(self, ds):
        k, d = ds2bytes(ds)
        d = self._comp.compress(d)
        return (k, d)

    def _doc2kv(self, ds_raw):
        k, d = doc2bytes(ds_raw)
        d = self._comp.compress(d)
        return (k, d)

    def _ds_save(self, ds, transaction):
        if ds.type.name not in self._products:
            self._products[ds.type.name] = ds.type

        k, v = self._ds2kv(ds)
        transaction.put(k, v)

    def bulk_save(self, dss):
        with self._dbs.main.begin(self._dbs.ds, write=True) as tr:
            for ds in dss:
                self._ds_save(ds, tr)

    def tee(self, dss):
        """Given a lazy stream of datasets persist them to disk and then pass through
        for further processing.
        """
        have_some = True
        n_max_per_transaction = 100

        while have_some:
            with self._dbs.main.begin(self._dbs.ds, write=True) as tr:
                have_some = False
                for ds in itertools.islice(dss, n_max_per_transaction):
                    have_some = True
                    self._ds_save(ds, tr)
                    yield ds

    def bulk_save_raw(self, raw_dss):
        with self._dbs.main.begin(self._dbs.ds, write=True) as tr:
            for raw_ds in raw_dss:
                k, v = self._doc2kv(raw_ds)
                tr.put(k, v)

    def _extract_ds(self, d):
        d = self._decomp.decompress(d)
        doc = json.loads(d)
        return doc2ds(doc, self._products)

    def get(self, uuid):
        if isinstance(uuid, str):
            uuid = UUID(uuid)

        key = key_to_bytes(uuid)

        with self._dbs.main.begin(self._dbs.ds) as tr:
            d = tr.get(key, None)
            if d is None:
                return None

            return self._extract_ds(d)

    def get_all(self):
        with self._dbs.main.begin(self._dbs.ds) as tr:
            for _, d in tr.cursor():
                yield self._extract_ds(d)


def maybe_delete_db(path):
    path = Path(path)
    if not path.exists():
        return False

    if path.is_dir():
        db, lock = [path/n for n in ["data.mdb", "lock.mdb"]]
    else:
        db = path
        lock = Path(str(path)+'-lock')

    if db.exists() and lock.exists():
        db.unlink()
        lock.unlink()

        if path.is_dir():
            path.rmdir()

    return True


def open_cache(path, products=None):
    """
    """

    subdir = Path(path).is_dir()

    db = lmdb.open(path,
                   subdir=subdir,
                   max_dbs=8,
                   lock=False,
                   readonly=True)

    try:
        db_info = db.open_db(b'info', create=False)
    except lmdb.NotFoundError:
        raise ValueError('Existing database is not a ds cache')

    with db.begin(db_info, write=False) as tr:
        version = tr.get(b'version', None)
        if version is None:
            raise ValueError('Missing format version field')
        if version != FORMAT_VERSION:
            raise ValueError("Unsupported on disk version: " + version.decode('utf8'))

        zdict = tr.get(b'zdict', None)
        if products is None:
            products = {}  # TODO: load products

    dbs = SimpleNamespace(main=db,
                          info=db_info,
                          groups=db.open_db(b'groups', create=False),
                          ds=db.open_db(b'ds', create=False),
                          udata=db.open_db(b'udata', create=False))

    if zdict is not None:
        comp_params = {'dict_data': zstandard.ZstdCompressionDict(zdict)}
    else:
        comp_params = {}

    comp = None
    decomp = zstandard.ZstdDecompressor(**comp_params)

    state = SimpleNamespace(dbs=dbs,
                            comp=comp,
                            decomp=decomp,
                            products=products)

    return DatasetCache(state)


def create_cache(path,
                 complevel=6,
                 zdict=None,
                 max_db_sz=None,
                 truncate=False):
    """
    """

    if truncate:
        maybe_delete_db(path)

    if max_db_sz is None:
        max_db_sz = 10*(1 << 30)

    db = lmdb.open(path,
                   max_dbs=8,
                   map_size=max_db_sz,
                   readonly=False)

    # If db is not empty load zdict from info
    if db.stat()['entries'] > 0:
        try:
            db_info = db.open_db(b'info', create=False)
        except lmdb.NotFoundError:
            raise ValueError('Existing database is not a ds cache')

        with db.begin(db_info, write=False) as tr:
            version = tr.get(b'version', None)
            if version is None:
                raise ValueError('Missing format version field')
            if version != FORMAT_VERSION:
                raise ValueError("Unsupported on disk version: " + version.decode('utf8'))

            zdict = tr.get(b'zdict', None)
            products = {}  # TODO: load products
    else:
        products = {}
        db_info = db.open_db(b'info', create=True)

        with db.begin(db_info, write=True) as tr:
            tr.put(b'version', FORMAT_VERSION)

            if zdict is not None:
                assert isinstance(zdict, bytes)
                tr.put(b'zdict', zdict)

    dbs = SimpleNamespace(main=db,
                          info=db_info,
                          groups=db.open_db(b'groups', create=True),
                          ds=db.open_db(b'ds', create=True),
                          udata=db.open_db(b'udata', create=True))

    if zdict is not None:
        comp_params = {'dict_data': zstandard.ZstdCompressionDict(zdict)}
    else:
        comp_params = {}

    comp = zstandard.ZstdCompressor(level=complevel, **comp_params)
    decomp = zstandard.ZstdDecompressor(**comp_params)

    state = SimpleNamespace(dbs=dbs,
                            comp=comp,
                            decomp=decomp,
                            products=products)

    return DatasetCache(state)


def test_key_to_value():

    for k in ("string", 217987, 215781587158712587, ("AAA", 3)):
        bb = key_to_bytes(k)
        assert isinstance(bb, bytes)

    assert key_to_bytes(UUID(bytes=b"0123456789ABCDEF")) == b"0123456789ABCDEF"
    assert key_to_bytes(b"88") == b"88"


def test_create_cache():
    ss = create_cache('tmp.lmdb', truncate=True)
    print(ss)
    del ss
    ss = open_cache('tmp.lmdb')
    print(ss)
