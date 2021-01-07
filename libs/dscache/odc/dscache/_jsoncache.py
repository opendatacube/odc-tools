from typing import (
    Optional,
    Tuple,
    List,
    Collection,
    Iterator,
    Union,
    Any,
    Dict,
    Iterable,
)
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

FORMAT_VERSION = b"0001"
RESERVED_INFO_KEYS = set(["version", "zdict"])

Prefix = Union[str, bytes]
LaxUUID = Union[str, UUID]
Document = Dict[str, Any]
Document_ = Union[Document, Tuple[Union[str, UUID], Document]]
MaybeTransaction = Optional[lmdb.Transaction]


def key_to_bytes(k: Any) -> bytes:
    if isinstance(k, str):
        return k.encode("utf8")
    if isinstance(k, UUID):
        return k.bytes
    if isinstance(k, bytes):
        return k
    if isinstance(k, int):
        if k.bit_length() < 32:
            return k.to_bytes(4, "big")
        elif k.bit_length() < 128:
            return k.to_bytes(16, "big")
        else:
            return str(k).encode("utf8")
    if isinstance(k, tuple):
        return functools.reduce(operator.add, map(key_to_bytes, k))

    raise ValueError("Key must be one of str|bytes|int|UUID|tuple")


def uuids2bytes(uu: Collection[UUID]) -> bytes:
    bb = bytearray(len(uu) * 16)
    for i, u in enumerate(uu):
        bb[i * 16 : (i + 1) * 16] = u.bytes
    return bytes(bb)


def bytes2uuids(bb: bytes) -> List[UUID]:
    n = len(bb) // 16
    return [UUID(bytes=bb[i * 16 : (i + 1) * 16]) for i in range(n)]


def prefix_visit(
    tr: lmdb.Transaction, prefix: Prefix, full_key: bool = False, db=None
) -> Iterator[Tuple[bytes, bytes]]:
    if isinstance(prefix, str):
        prefix = prefix.encode("utf8")

    n = len(prefix)
    cursor = tr.cursor(db)
    cursor.set_range(prefix)
    for k, v in cursor:
        if len(k) < n or k[:n] != prefix:
            break
        yield (k if full_key else k[n:]), v


def dict2jsonKV(
    oo: Document,
    prefix: Optional[str] = None,
    compressor: Optional[zstandard.ZstdCompressor] = None,
) -> Iterator[Tuple[bytes, bytes]]:
    for k, doc in oo.items():
        data = json.dumps(doc, separators=(",", ":")).encode("utf8")
        if compressor is not None:
            data = compressor.compress(data)
        if prefix is not None:
            k = prefix + k

        yield (key_to_bytes(k), data)


def jsonKV2dict(
    kv: Iterable[Tuple[bytes, bytes]],
    decompressor: Optional[zstandard.ZstdDecompressor] = None,
) -> Document:
    def decode(kv):
        k, doc = kv
        if decompressor is not None:
            doc = decompressor.decompress(doc)
        return k.decode("utf8"), json.loads(doc)

    doc = {k: doc for k, doc in map(decode, kv)}
    if len(doc) == 1 and list(doc)[0] == "":
        doc = doc[""]
    return doc


def doc2bytes(doc: Document_, purge_id: bool = False) -> Tuple[bytes, bytes]:
    """doc is

    Either:

    1. (id, {..}) key-value tuple
    2. {id: , ..}  dict with `id` field

    id has to be a UUID (typically string)
    """
    if isinstance(doc, tuple):
        k, d = doc
    else:
        k_ = doc.get("id")
        if not isinstance(k_, (str, UUID)):
            raise ValueError("Expect id key to be a string or a UUID")

        k = k_
        d = toolz.dissoc(doc, ["id"]) if purge_id else doc

    if not isinstance(k, UUID):
        if not isinstance(k, str):
            raise ValueError("Expect id key to be a string or a UUID")

        k = UUID(k)

    raw_k = k.bytes
    raw_doc = json.dumps(d, separators=(",", ":")).encode("utf8")
    return (raw_k, raw_doc)


class JsonBlobCache:
    """
    info:
       version: 4-bytes
       zdict: pre-trained compression dictionary, optional

    groups:
       Each group is named list of uuids

    udata:
       arbitrary user data (TODO)

    ds:
       uuid: compressed(json({..}))
    """

    def __init__(self, state):
        """Don't use this directly, use create_cache or open_(rw|ro)."""

        self._dbs = state.dbs
        self._comp: Optional[zstandard.ZstdCompressor] = state.comp
        self._decomp: zstandard.ZstdCompressor = state.decomp
        self._closed = False
        self._current_transaction: MaybeTransaction = None

    def close(self):
        """Write any outstanding product/metadata definitions (if in write mode) and
        close database file.

        """
        if not self._closed:
            try:
                self._dbs.main.close()
            finally:
                self._closed = True

    def __del__(self):
        self.close()

    @staticmethod
    def train_dictionary(
        docs: Iterable[Document_], dict_sz: int = 8 * 1024
    ) -> Optional[bytes]:
        """Given a finite sequence of Documents train zstandard compression dictionary of a given size.

        Document is either a
         - (id, {..}) tuple
         - {id: str|UUID, ...} a dictionary with `id` key of type UUID (possibly string formatted UUID)

        Will return None if input sequence is empty.
        """
        sample = list(v for _, v in map(doc2bytes, docs))

        if len(sample) == 0:
            return None

        return zstandard.train_dictionary(dict_sz, sample).as_bytes()

    def _append_info_dict(self, prefix: str, oo: Document, tr: lmdb.Transaction):
        for k, v in dict2jsonKV(oo, prefix, self._comp):
            tr.put(k, v, overwrite=True, dupdata=False, db=self._dbs.info)

    def _clear_info_dict(self, prefix: Prefix, tr: lmdb.Transaction):
        db_info = self._dbs.info
        for k, _ in prefix_visit(tr, prefix, full_key=True, db=db_info):
            tr.delete(k, db=db_info)

    def _get_info_dict(self, prefix: Prefix, tr: lmdb.Transaction) -> Document:
        return jsonKV2dict(prefix_visit(tr, prefix, db=self._dbs.info), self._decomp)

    def append_info_dict(
        self, prefix: str, oo: Document, transaction: MaybeTransaction = None
    ):
        if prefix in RESERVED_INFO_KEYS:
            raise ValueError(
                f"Prefix '{prefix}' can not be used, it is reserved for internal use"
            )

        if transaction is None:
            with self._dbs.main.begin(write=True) as tr:
                self._append_info_dict(prefix, oo, tr)
        else:
            self._append_info_dict(prefix, oo, transaction)

    def get_info_dict(
        self, prefix: str, transaction: MaybeTransaction = None
    ) -> Document:
        if prefix in RESERVED_INFO_KEYS:
            raise ValueError(
                f"Prefix '{prefix}' can not be read, it is reserved for internal use"
            )

        if transaction is None:
            with self._dbs.main.begin(write=False) as tr:
                return self._get_info_dict(prefix, tr)
        else:
            return self._get_info_dict(prefix, transaction)

    def clear_info_dict(self, prefix: str, transaction: MaybeTransaction = None):
        if prefix in RESERVED_INFO_KEYS:
            raise ValueError(
                f"Prefix '{prefix}' can not be cleared, it is reserved for internal use"
            )

        if transaction is None:
            with self._dbs.main.begin(write=True) as tr:
                return self._clear_info_dict(prefix, tr)
        else:
            return self._clear_info_dict(prefix, transaction)

    def get_info_keys(self, prefix: str = "") -> Iterator[str]:
        with self._dbs.main.begin(self._dbs.info, write=False, buffers=True) as tr:
            if prefix == "":
                kv_stream = tr.cursor()
            else:
                kv_stream = prefix_visit(tr, prefix, full_key=True)

            for k, _ in kv_stream:
                ks = bytes(k).decode("utf8")
                if ks not in RESERVED_INFO_KEYS:
                    yield ks

    @property
    def current_transaction(self) -> MaybeTransaction:
        return self._current_transaction

    @property
    def readonly(self) -> bool:
        return self._comp is None

    def _doc2kv(self, doc: Document_) -> Tuple[bytes, bytes]:
        k, d = doc2bytes(doc)
        if self._comp:
            d = self._comp.compress(d)
        return (k, d)

    def _doc_save(self, doc: Document_, transaction: lmdb.Transaction):
        k, v = self._doc2kv(doc)
        transaction.put(k, v)

    def bulk_save(self, docs: Iterable[Document_]):
        try:
            with self._dbs.main.begin(self._dbs.ds, write=True) as tr:
                self._current_transaction = tr
                for doc in docs:
                    self._doc_save(doc, tr)
        finally:
            self._current_transaction = None

    def tee(
        self, docs: Iterable[Any], max_transaction_size: int = 10000, transform=None
    ) -> Iterator[Any]:
        """Given a lazy stream of (k,v) pairs persist them to disk and then pass through
        for further processing.
        :docs: stream of documents (uuid, {..}) pairs
        :max_transaction_size int: How often to commit results to disk
        :transform: If docs contains some other objects you can supply mapping from that Data type to Tuple[uuid, dict]
        """
        have_some = True
        if transform is None:
            transform = lambda x: x

        try:
            while have_some:
                with self._dbs.main.begin(self._dbs.ds, write=True) as tr:
                    self._current_transaction = tr
                    have_some = False
                    for ds in itertools.islice(docs, max_transaction_size):
                        have_some = True
                        self._doc_save(transform(ds), tr)
                        yield ds
        finally:
            self._current_transaction = None

    def put_group(self, name: str, uuids: Collection[UUID]):
        """Group is a named list of uuids"""
        data = uuids2bytes(uuids)
        k = key_to_bytes(name)

        with self._dbs.main.begin(self._dbs.groups, write=True) as tr:
            tr.put(k, data)

    def _get_group_raw(self, name: Prefix) -> bytes:
        k = key_to_bytes(name)

        with self._dbs.main.begin(self._dbs.groups, write=False) as tr:
            return tr.get(k)

    def get_group(self, name: str) -> Optional[List[UUID]]:
        """Group is a named list of uuids"""
        data = self._get_group_raw(key_to_bytes(name))
        return bytes2uuids(data) if data is not None else None

    def groups(
        self, raw: bool = False, prefix: Optional[Prefix] = None
    ) -> Union[List[Tuple[bytes, int]], List[Tuple[str, int]]]:
        """Get list of tuples (group_name, group_size).

        :raw bool: Normally names are returned as strings, supplying raw=True
        would return bytes instead, this is needed if you are using group names
        that are not strings, like integers or tuples of basic types.

        :prefix str|bytes: Only report groups with name starting with prefix
        """

        assert isinstance(prefix, (str, bytes, type(None)))

        def _raw(prefix):
            with self._dbs.main.begin(
                self._dbs.groups, write=False, buffers=True
            ) as tr:
                cursor = (
                    tr.cursor()
                    if prefix is None
                    else prefix_visit(tr, prefix, full_key=True)
                )
                return [(bytes(k), len(d) // 16) for k, d in cursor]

        if prefix is not None:
            prefix = key_to_bytes(prefix)

        nn = _raw(prefix)
        return nn if raw else [(n.decode("utf8"), c) for n, c in nn]

    def _extract_ds(self, d: bytes) -> Document:
        d = self._decomp.decompress(d)
        return json.loads(d)

    def get(self, uuid: LaxUUID) -> Optional[Document]:
        """Extract single dataset with a given uuid, or return None if not found"""
        if isinstance(uuid, str):
            uuid = UUID(uuid)

        key = key_to_bytes(uuid)

        with self._dbs.main.begin(self._dbs.ds, buffers=True) as tr:
            d = tr.get(key, None)
            if d is None:
                return None

            return self._extract_ds(d)

    def get_all(self) -> Iterator[Tuple[UUID, Document]]:
        try:
            with self._dbs.main.begin(self._dbs.ds, buffers=True) as tr:
                self._current_transaction = tr
                for k, d in tr.cursor():
                    yield UUID(bytes=bytes(k)), self._extract_ds(d)
        finally:
            self._current_transaction = None

    def stream_group(self, group_name: str) -> Iterator[Tuple[UUID, Document]]:
        uu = self._get_group_raw(group_name)
        if uu is None:
            raise ValueError("No such group: %s" % group_name)

        if len(uu) & 0xF:
            raise ValueError("Wrong data size for group %s" % group_name)

        with self._dbs.main.begin(self._dbs.ds, buffers=True) as tr:
            for i in range(0, len(uu), 16):
                key = uu[i : i + 16]
                d = tr.get(key, None)
                if d is None:
                    raise ValueError("Missing dataset for %s" % (str(UUID(bytes=key))))

                yield UUID(bytes=bytes(key)), self._extract_ds(d)

    @property
    def count(self) -> int:
        with self._dbs.main.begin(self._dbs.ds) as tr:
            return tr.stat()["entries"]

    @property
    def path(self) -> Path:
        return Path(self._dbs.main.path())

    @staticmethod
    def open_ro(path: str, lock: bool = False) -> "JsonBlobCache":
        return open_ro(path, lock=lock)

    @staticmethod
    def open_rw(path: str, **kw) -> "JsonBlobCache":
        return open_rw(path, **kw)

    @staticmethod
    def create(
        path: str,
        complevel: int = 6,
        zdict: Optional[bytes] = None,
        max_db_sz: Optional[int] = None,
        lock: bool = False,
        subdir: bool = False,
        truncate: bool = False,
        **kw,
    ) -> "JsonBlobCache":
        """Create new file database or open existing one.

        :path str: Path where to create new database (this will be a directory with 2 files in it)
        :complevel int: Level of compressions to apply per dataset, bigger is slower but better compression.
        :zdict: Optional pre-trained compression dictionary
        :max_db_sz int: Maximum size in bytes (defaults to 10GiB)
        :lock: By default we assume exclusive access to the file, if you expect sharing set ``lock=True``
        :subdir: If set to True treat path as directory

        :truncate bool: If True wipe out any existing database and create new empty one.
        :kw: Passed on to ``lmdb.open(.., **kw)``
           meminit
            writemap
            sync
            metasync
            ...and other
        """

        return create_cache(
            path,
            complevel=complevel,
            zdict=zdict,
            max_db_sz=max_db_sz,
            lock=lock,
            subdir=subdir,
            truncate=truncate,
            **kw,
        )

    @staticmethod
    def exists(path: str) -> bool:
        return db_exists(path)


def db_exists(path: str) -> bool:
    path = Path(path)
    if not path.exists():
        return False

    if path.is_dir():
        return Path(path / "data.mdb").exists()

    return True


def maybe_delete_db(path: str) -> bool:
    """Delete existing database if it exists.

     LMDB database consists of two files, data + lock, they can be arranged in two possible layouts:

     - `db-dir-name/{data.mdb, lock.mdb}`
     - `db-file-name` and `db-file-name-lock`

    You supply path which is either `db-dir-name` or `db-file-name`.
    """
    path = Path(path)
    if not path.exists():
        return False

    if path.is_dir():
        db, lock = [path / n for n in ["data.mdb", "lock.mdb"]]
    else:
        db = path
        lock = Path(str(path) + "-lock")

    if db.exists() and lock.exists():
        db.unlink()
        lock.unlink()

        if path.is_dir():
            path.rmdir()

    return True


def _from_existing_db(db, complevel: int = 6) -> JsonBlobCache:
    readonly = db.flags().get("readonly")

    try:
        db_info = db.open_db(b"info", create=False)
    except lmdb.NotFoundError:
        raise ValueError("Existing database is not a ds cache")

    with db.begin(db_info, write=False) as tr:
        version = tr.get(b"version", None)
        if version is None:
            raise ValueError("Missing format version field")
        if version != FORMAT_VERSION:
            raise ValueError("Unsupported on disk version: " + version.decode("utf8"))

        zdict = tr.get(b"zdict", None)

    dbs = SimpleNamespace(
        main=db,
        info=db_info,
        groups=db.open_db(b"groups", create=False),
        ds=db.open_db(b"ds", create=False),
        udata=db.open_db(b"udata", create=False),
    )

    comp_params = {"dict_data": zstandard.ZstdCompressionDict(zdict)} if zdict else {}

    comp = (
        None if readonly else zstandard.ZstdCompressor(level=complevel, **comp_params)
    )
    decomp = zstandard.ZstdDecompressor(**comp_params)

    state = SimpleNamespace(dbs=dbs, comp=comp, decomp=decomp)

    return JsonBlobCache(state)


def _from_empty_db(db, complevel: int = 6, zdict: Optional[bytes] = None):
    assert isinstance(zdict, (bytes, type(None)))

    db_info = db.open_db(b"info", create=True)

    with db.begin(db_info, write=True) as tr:
        tr.put(b"version", FORMAT_VERSION)

        if zdict is not None:
            tr.put(b"zdict", zdict)

    dbs = SimpleNamespace(
        main=db,
        info=db_info,
        groups=db.open_db(b"groups", create=True),
        ds=db.open_db(b"ds", create=True),
        udata=db.open_db(b"udata", create=True),
    )

    comp_params = {"dict_data": zstandard.ZstdCompressionDict(zdict)} if zdict else {}

    comp = zstandard.ZstdCompressor(level=complevel, **comp_params)
    decomp = zstandard.ZstdDecompressor(**comp_params)

    state = SimpleNamespace(dbs=dbs, comp=comp, decomp=decomp)

    return JsonBlobCache(state)


def open_ro(path: str, lock: bool = False, **kw) -> JsonBlobCache:
    """Open existing database in readonly mode.

    .. note:

       default mode assumes db file is static (not being modified
       externally), if this is not the case, supply ``lock=True`` parameter.

    :path: Path to the db could be folder or actual file

    :lock: Supply ``True`` if external process is changing DB concurrently.
    """

    subdir = Path(path).is_dir()

    db = lmdb.open(
        path, subdir=subdir, max_dbs=8, lock=lock, create=False, readonly=True, **kw
    )

    return _from_existing_db(db)


def open_rw(
    path: str,
    max_db_sz: Optional[int] = None,
    complevel: int = 6,
    lock: bool = False,
    **kw,
) -> JsonBlobCache:
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

    subdir = Path(path).is_dir()

    if max_db_sz is None:
        max_db_sz = 10 * (1 << 30)

    db = lmdb.open(
        path,
        subdir=subdir,
        max_dbs=8,
        map_size=max_db_sz,
        lock=lock,
        create=False,
        readonly=False,
        **kw,
    )

    return _from_existing_db(db, complevel=complevel)


def create_cache(
    path: str,
    complevel: int = 6,
    zdict: Optional[bytes] = None,
    max_db_sz: Optional[int] = None,
    truncate: bool = False,
    lock: bool = False,
    subdir: bool = False,
    **kw,
) -> JsonBlobCache:
    """Create new file database or open existing one.

    :path str: Path where to create new database (this will be a directory with 2 files in it)
    :complevel int: Level of compressions to apply per dataset, bigger is slower but better compression.
    :zdict: Optional pre-trained compression dictionary
    :max_db_sz int: Maximum size in bytes (defaults to 10GiB)
    :lock: By default we assume exclusive access to the file, if you expect sharing set ``lock=True``
    :subdir: If set to True treat path as directory

    :truncate bool: If True wipe out any existing database and create new empty one.
    :kw: Passed on to ``lmdb.open(.., **kw)``
      meminit
      writemap
      sync
      metasync
      ...and other

    """

    if truncate:
        maybe_delete_db(path)

    if max_db_sz is None:
        max_db_sz = 10 * (1 << 30)

    db = lmdb.open(
        path,
        max_dbs=8,
        map_size=max_db_sz,
        create=True,
        readonly=False,
        lock=lock,
        subdir=subdir,
        **kw,
    )

    # If db is not empty just call open on it
    if db.stat()["entries"] > 0:
        return _from_existing_db(db, complevel=complevel)
    else:
        return _from_empty_db(db, complevel=complevel, zdict=zdict)


def test_key_to_value():

    for k in ("string", 217987, 215781587158712587, ("AAA", 3)):
        bb = key_to_bytes(k)
        assert isinstance(bb, bytes)

    assert key_to_bytes(UUID(bytes=b"0123456789ABCDEF")) == b"0123456789ABCDEF"
    assert key_to_bytes(b"88") == b"88"


def test_create_cache():
    ss = create_cache("tmp.lmdb", truncate=True)
    print(ss)
    del ss
    ss = open_ro("tmp.lmdb")
    print(ss)
