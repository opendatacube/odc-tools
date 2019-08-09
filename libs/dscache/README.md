# Dataset Cache

Random access cache of `Dataset` objects backed by disk storage.

- Uses `lmdb` as key value store
  - UUID is the key
  - Compressed json blob is value
- Uses `zstandard` compression (with pre-trained dictionaries)
  - Achieves pretty good compression (db size is roughly 3 times larger than `.tar.gz` of dataset yaml files), but, unlike tar archive, allows random access.
- Keeps track of `Product` and `Metadata` objects
- Has concept of "groups" (used for `GridWorkFlow`)


## Installation

```
pip install --extra-index-url="https://packages.dea.gadevs.ga" odc_dscache
```

## Exporting from Datacube

### Using command line app

There is a CLI tool called `slurpy` that can export a set of products to a file

```
> slurpy --help
Usage: slurpy [OPTIONS] OUTPUT [PRODUCTS]...

Options:
  -E, --env TEXT  Datacube environment name
  -z INTEGER      Compression setting for zstandard 1-fast, 9+ good but slow
  --help          Show this message and exit.
```

Note that this app is not affected by [issue#542](https://github.com/opendatacube/datacube-core/issues/542), as it implements a properly lazy SQL query using cursors.


### From python code

```python
from odc import dscache

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
from odc import dscache

cache = dscache.open_ro('sample.db')

# access individual dataset: returns None if not found
ds = cache.get('005b0ab7-5454-4eef-829d-ed081135aefb')
if ds is not None:
   do_stuff_with(ds)

# stream all datasets
for ds in cache.get_all():
   do_stuff_with(ds)
```

For more details see [notebook](../notebooks/dscache-example.ipynb).

## Groups

Group is a collection of datasets that are somehow related. It is essentially a simple index: a list of uuids stored under some name. For example we might want to group all datasets that overlap a certain Albers tile into a group with a name `albers/{x}_{y}`. One can query a list of all group names with `.groups()` method. One can add new group using `.put_group(name, list_of_uuids)`. To read all datasets that belong to a given group `.stream_group(group_name)` can be used.

- Get list of group names and their population counts: `.groups() -> List((name, count))`
- Get datasets for a given group: `.stream_group(group_name) -> lazy sequence of Dataset objects`
- To get just uuids: `.get_group(group_name) -> List[UUID]`

There is a cli tool `dstiler` that can group datasets based on `GridSpec`

```
Usage: dstiler [OPTIONS] DBFILE

  Add spatial grouping to file db.

  Default grid is Australian Albers (EPSG:3577) with 100k by 100k tiles. But
  you can also group by Landsat path/row (--native), or Google's map tiling
  regime (--web zoom_level)

Options:
  --native         Use Landsat Path/Row as grouping
  --native-albers  When datasets are in Albers grid already
  --web INTEGER    Use web map tiling regime at supplied zoom level
  --help           Show this message and exit.
```

Note that unlike tools like `datacube-stats --save-tasks` that rely on `GridWorkflow.group_into_cells`, `dstiler` is capable of processing large datasets since it does not keep the entire `Dataset` object in memory for every dataset observed, instead only UUID is kept in RAM until completion, drastically reducing RAM usage. There is also an optimization for ingested products, these are already tiled into Albers tiles so rather than doing relatively expensive geometry overlap checks we can simply extract Albers tile index directly from `Dataset`'s  `.metadata.grid_spatial` property. To use this option supply `--native-albers` to `dstiler` app.


## Notes on performance

It took 26 minutes to slurp 2,627,779 wofs datasets from a local postgres server on AWS(`r4.xlarge`), this generated 1.4G database file.

```
Command being timed: "slurpy -E wofs wofs.db :all:"
User time (seconds): 1037.93
System time (seconds): 48.77
Percent of CPU this job got: 69%
Elapsed (wall clock) time (h:mm:ss or m:ss): 26:04.79
```

Adding Albers tile grouping to this took just over 4 minutes, that's a processing rate of ~10.6K datasets per second.

```
Command being timed: "dstiler --native-albers wofs.db"
User time (seconds): 234.57
System time (seconds): 2.65
Percent of CPU this job got: 95%
Elapsed (wall clock) time (h:mm:ss or m:ss): 4:08.70
```

Similar work load but on VDI node (2,747,870 wofs dataset from main db) took 23 minutes to dump all datasets from DB and 7 minutes to tile into Albers grid using "native grid" optimization. Read throughput from file db on VDI node is slower than on AWS, but is still a respectable 6.5K datasets per second. Database file was somewhat bigger too, 2G vs 1.4G on AWS, maybe there is a significant difference in `zstandard` library between two systems. 

```
Command being timed: "slurpy wofs.db wofs_albers"
User time (seconds): 1077.74
System time (seconds): 49.75
Percent of CPU this job got: 81%
Elapsed (wall clock) time (h:mm:ss or m:ss): 23:01.20
```

```
Command being timed: "dstiler --native-albers wofs.db"
User time (seconds): 408.65
System time (seconds): 6.28
Percent of CPU this job got: 98%
Elapsed (wall clock) time (h:mm:ss or m:ss): 7:03.22
```

I'd like to point out that grouping datasets into Grids can very well happen during `slurpy` process without adding much overhead, so two step processing is not strictly necessary.
