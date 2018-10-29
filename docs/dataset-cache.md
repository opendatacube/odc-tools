# Dataset Cache

Random access cache of `Dataset` objects backed by disk storage.

- Uses `lmdb` as key value store
  - UUID is the key
  - Compressed json blob is value
- Uses `zstandard` compression (with pre-trained dictionaries)
  - Achieves pretty good compression (db size is roughly 2 times larger than `.tar.gz` of dataset yaml files), but, unlike tar archive, allows random access.
- Keeps track of `Product` and `Metadata` objects
- Has concept of "groups" (used for `GridWorkFlow`)


## Exporting from Datacube

### Using command line app

There is a CLI tool called `slurpy` that can export a set of products to a file

```
> slurpy --help
Usage: slurpy [OPTIONS] OUTPUT [PRODUCTS]...

Options:
  -E, --env TEXT  Datacube environment name
  --help          Show this message and exit.
```

Note that this app is not affected by [issue#542](https://github.com/opendatacube/datacube-core/issues/542), as it implements a properly lazy SQL query using cursors.


### From python code

```python
from dea import dscache

# create new file db, deleting old one if exists
cache = dscache.create_cache('sample.db', truncate=True)

# dataset stream from some query
dss = dc.find_datasets_lazy(..)

# tee off dataset stream into db file
dss = cache.tee(dss)

# then just process the stream of datasets
for ds in dss:
   do_stuff_with(ds)

# finally you can call `.close`
cache.close()
```

## Reading from a file database

By default we assume that database file is read-only. If however some other process is writing to the db while this process is reading, you have to supply extra argument to `open_ro(.., lock=True)`. You better not do that over network file system.

```python
from dea import dscache

cache = dscache.open_ro('sample.db')

# access individual dataset: returns None if not found
ds = cache.get('005b0ab7-5454-4eef-829d-ed081135aefb')
if ds is not None:
   do_stuff_with(ds)

# stream all datasets
for ds in cache.get_all():
   do_stuff_with(ds)
```

## Groups

Group is a collection of datasets that are somehow related. It is essentially a simple index: a list of uuids stored under some name. For example we might want to group all datasets that overlap a certain Albers tile into a group with a name `albers/{x}_{y}`. One can query a list of all group names with `.groups()` method. One can add new group using `.put_group(name, list_of_uuids)`. To read all datasets that belong to a given group `.stream_group(group_name)` can be used.

- Get list of group names and their population counts: `.groups() -> List((name, count))`
- Get datasets for a given group: `.stream_group(group_name) -> lazy sequence of Dataset objects`
- To get just uuids: `.get_group(group_name) -> List[UUID]`
