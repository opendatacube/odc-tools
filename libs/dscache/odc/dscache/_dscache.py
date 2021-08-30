from typing import (
    Tuple,
    Dict,
    List,
    Union,
    Iterator,
    Optional,
    Iterable,
    Collection,
    cast,
)
from uuid import UUID
from pathlib import Path

import toolz
from datacube.model import (
    Dataset,
    GridSpec,
    DatasetType,
    MetadataType,
    metadata_from_doc,
)
from datacube.utils.geometry import CRS
from . import _jsoncache as base

from odc.io.text import split_and_check

# pylint: disable=invalid-name,too-many-public-methods

ProductCollection = Union[
    Iterator[DatasetType], List[DatasetType], Dict[str, DatasetType]
]
Document = base.Document
LaxUUID = base.LaxUUID
TileIdx = Union[Tuple[int, int], Tuple[str, int, int]]


def ds2doc(ds) -> Tuple[UUID, Document]:
    return (ds.id, dict(uris=ds.uris, product=ds.type.name, metadata=ds.metadata_doc))


def doc2ds(
    doc: Optional[Document], products: Dict[str, DatasetType]
) -> Optional[Dataset]:
    if doc is None:
        return None

    p = products.get(doc["product"], None)
    if p is None:
        raise ValueError("No product named: %s" % doc["product"])
    return Dataset(p, doc["metadata"], uris=doc["uris"])


def gs2doc(gs: GridSpec) -> base.Document:
    return dict(
        crs=str(gs.crs),
        tile_size=list(gs.tile_size),
        resolution=list(gs.resolution),
        origin=list(gs.origin),
    )


def doc2gs(doc: Document) -> GridSpec:
    return GridSpec(
        crs=CRS(doc["crs"]),
        tile_size=tuple(doc["tile_size"]),
        resolution=tuple(doc["resolution"]),
        origin=tuple(doc["origin"]),
    )


def build_dc_product_map(
    metadata_json: Document, products_json: Document
) -> Tuple[Dict[str, MetadataType], Dict[str, DatasetType]]:
    mm = toolz.valmap(metadata_from_doc, metadata_json)

    def mk_product(doc, name):
        mt = doc.get("metadata_type")
        if mt is None:
            raise ValueError("Missing metadata_type key in product definition")
        metadata = mm.get(mt)

        if metadata is None:
            raise ValueError("No such metadata %s for product %s" % (mt, name))

        return DatasetType(metadata, doc)

    return mm, {k: mk_product(doc, k) for k, doc in products_json.items()}


def _metadata_from_products(
    products: Dict[str, DatasetType]
) -> Dict[str, MetadataType]:
    mm = {}
    for p in products.values():
        m = p.metadata_type
        if m.name not in mm:
            mm[m.name] = m

    return mm


def mk_group_name(idx: TileIdx, name: str = "unnamed_grid") -> str:
    x, y = idx[-2:]
    if len(idx) == 2:
        return f"{name}/{x:+05d}/{y:+05d}"
    elif len(idx) == 3:
        t = idx[0]
        return f"{name}/{t}/{x:+05d}/{y:+05d}"
    raise ValueError("Expect index in (x, y) or (t, x, y) format")


def parse_group_name(group_name: str) -> Tuple[TileIdx, str]:
    """Return an
      ((int, int), prefix:str)       x,y
      ((str, int, int), prefix:str)  t,x,y

    tuple from group name.

    Expects group to be in the form
     {prefix}/{x}/{y}
    or
     {prefix}/{t}/{x}/{y}

    raises ValueError if group_name is not in the expected format.
    """

    try:
        prefix, *tidx = split_and_check(group_name, "/", (3, 4))
        x, y = map(int, tidx[-2:])
    except ValueError:
        raise ValueError("Bad group name: " + group_name) from None

    if len(tidx) == 2:
        return (x, y), prefix

    return (cast(str, tidx[0]), x, y), prefix


class DatasetCache:
    """
    info:
       version: 4-bytes
       zdict: pre-trained compression dictionary, optional
       product/{name}: json
       metadata/{name}: json
       grid/{name}: json

    groups:
       Each group is named list of uuids

    udata:
       arbitrary user data (TODO)

    ds:
       uuid: compressed(json({product: str,
                              uris: [str],
                              metadata: object}))
    """

    def __init__(
        self, db: base.JsonBlobCache, products: Optional[ProductCollection] = None
    ):
        """Don't use this directly, use create_cache or open_(rw|ro)."""

        if products is None:
            metadata, products = build_dc_product_map(
                db.get_info_dict("metadata/"), db.get_info_dict("product/")
            )
        else:
            if not isinstance(products, dict):
                products = {p.name: p for p in products}

            metadata = _metadata_from_products(products)

        self._db = db
        self._products = products
        self._metadata = metadata

    def close(self):
        """Write any outstanding product/metadata definitions (if in write mode) and
        close database file.

        """
        self._db.close()

    @staticmethod
    def train_dictionary(dss: Iterator[Dataset], dict_sz=8 * 1024) -> Optional[bytes]:
        """Given a finite sequence of Datasets train zstandard compression dictionary of a given size.

        Accepts both `Dataset` as well as "raw" datasets.

        Will return None if input sequence is empty.
        """
        docs = list(map(ds2doc, dss))
        return base.JsonBlobCache.train_dictionary(docs, dict_sz=dict_sz)

    @property
    def readonly(self) -> bool:
        return self._db.readonly

    @property
    def count(self) -> int:
        return self._db.count

    def put_group(self, name: str, uuids: Collection[UUID]):
        """Group is a named list of uuids"""
        self._db.put_group(name, uuids)

    def get_group(self, name: str) -> Optional[List[UUID]]:
        """Group is a named list of uuids"""
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
    def products(self) -> Dict[str, DatasetType]:
        return self._products

    @property
    def metadata(self) -> Dict[str, MetadataType]:
        return self._metadata

    def _add_metadata(self, metadata: MetadataType, transaction: base.MaybeTransaction):
        self._metadata[metadata.name] = metadata
        self._db.append_info_dict(
            "metadata/", {metadata.name: metadata.definition}, transaction
        )

    def _add_product(self, product: DatasetType, transaction: base.MaybeTransaction):
        if product.metadata_type.name not in self._metadata:
            self._add_metadata(product.metadata_type, transaction)

        self._products[product.name] = product
        self._db.append_info_dict(
            "product/", {product.name: product.definition}, transaction
        )

    def _ds2doc(self, ds: Dataset) -> Tuple[UUID, Document]:
        if ds.type.name not in self._products:
            self._add_product(ds.type, self._db.current_transaction)
        return ds2doc(ds)

    def bulk_save(self, dss: Iterable[Dataset]):
        docs = (self._ds2doc(ds) for ds in dss)
        return self._db.bulk_save(docs)

    def tee(
        self, dss: Iterable[Dataset], max_transaction_size: int = 10000
    ) -> Iterator[Dataset]:
        """Given a lazy stream of datasets persist them to disk and then pass through
        for further processing.
        :dss: stream of datasets
        :max_transaction_size int: How often to commit results to disk
        """
        return self._db.tee(
            dss, max_transaction_size=max_transaction_size, transform=self._ds2doc
        )

    def get(self, uuid: LaxUUID) -> Dataset:
        """Extract single dataset with a given uuid, or return None if not found"""
        return doc2ds(self._db.get(uuid), self._products)

    def get_all(self) -> Iterator[Dataset]:
        for _, v in self._db.get_all():
            yield doc2ds(v, self._products)

    def stream_group(self, group_name: str) -> Iterator[Dataset]:
        for _, v in self._db.stream_group(group_name):
            yield doc2ds(v, self._products)

    @property
    def grids(self) -> Dict[str, GridSpec]:
        """Grids defined for this dataset cache"""
        return {
            key: doc2gs(value) for key, value in self._db.get_info_dict("grid/").items()
        }

    def add_grid(self, gs: GridSpec, name: str):
        """Register a grid"""
        self._db.append_info_dict("grid/", {name: gs2doc(gs)})

    def add_grid_tile(self, grid: str, idx: TileIdx, uuids: Collection[UUID]):
        """Add list of dataset UUIDs to a tile"""
        key = mk_group_name(idx, grid)
        self._db.put_group(key, uuids)

    def add_grid_tiles(self, grid: str, tiles: Dict[TileIdx, List[UUID]]):
        """Add multiple tiles to a grid"""
        for idx, uuids in tiles.items():
            self.add_grid_tile(grid, idx, uuids)

    def tiles(self, grid: str) -> List[Tuple[TileIdx, int]]:
        """Return tile indexes and dataset counts"""

        def tile_index(group_name):
            idx, prefix = parse_group_name(group_name)
            assert prefix == grid
            return idx

        return [
            (tile_index(group_name), count)
            for group_name, count in self.groups(prefix=grid + "/")
        ]

    def stream_grid_tile(self, idx: TileIdx, grid: str) -> Iterator[Dataset]:
        """Iterate over datasets in a given tile"""
        return self.stream_group(mk_group_name(idx, grid))

    def append_info_dict(
        self, prefix: str, oo: Document, transaction: base.MaybeTransaction = None
    ):
        self._db.append_info_dict(prefix, oo, transaction=transaction)

    def get_info_dict(
        self, prefix: str, transaction: base.MaybeTransaction = None
    ) -> Document:
        return self._db.get_info_dict(prefix, transaction=transaction)

    def clear_info_dict(self, prefix: str, transaction: base.MaybeTransaction = None):
        return self._db.clear_info_dict(prefix, transaction=transaction)

    @property
    def path(self) -> Path:
        return self._db.path

    @staticmethod
    def open_ro(path: str, lock: bool = False) -> "DatasetCache":
        return open_ro(path, lock=lock)

    @staticmethod
    def open_rw(path: str, **kw) -> "DatasetCache":
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
    ) -> "DatasetCache":
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
        return base.db_exists(path)


def open_ro(
    path: str, products: Optional[ProductCollection] = None, lock: bool = False, **kw
) -> DatasetCache:
    """Open existing database in readonly mode.

    .. note::

    default mode assumes db file is static (not being modified
    externally), if this is not the case, supply `lock=True` parameter.

    :path str: Path to the db could be folder or actual file

    :products: Override product dictionary with compatible products loaded from
    the datacube database, this is generally only needed if you intend to add
    datasets to the datacube index directly (i.e. without product matching
    metadata documents).

    :lock bool: Supply True if external process is changing DB concurrently.

    """

    db = base.open_ro(path, lock=lock, **kw)
    return DatasetCache(db, products=products)


def open_rw(
    path: str,
    products: Optional[ProductCollection] = None,
    max_db_sz: Optional[int] = None,
    complevel: int = 6,
    lock: bool = False,
    **kw,
) -> DatasetCache:
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
    db = base.open_rw(path, max_db_sz=max_db_sz, complevel=complevel, lock=lock, **kw)
    return DatasetCache(db, products)


def create_cache(
    path: str,
    complevel: int = 6,
    zdict: Optional[bytes] = None,
    max_db_sz: Optional[int] = None,
    truncate: bool = False,
    lock: bool = False,
    subdir: bool = False,
    **kw,
) -> DatasetCache:
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
    db = base.create_cache(
        path,
        complevel=complevel,
        zdict=zdict,
        max_db_sz=max_db_sz,
        truncate=truncate,
        lock=lock,
        subdir=subdir,
        **kw,
    )
    return DatasetCache(db)
