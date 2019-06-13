# ODC Enhancement: Replace the Database API

**Date** 2019/06/12

**Author** Kirill Kouzoubov  (@Kirill888)

**Version** >= ODC 1.7

# Summary

The current Database API, used throughout the ODC Core codebase, is
responsible for all access to the ODC Index. It was designed to be highly
flexible in terms of Metadata it can handle, while maintaining high
performance access to search across multiple dimensions.

Unfortunately, it is showing it's age, and has several assumptions and design
flaws which are limiting future ODC development.

This proposal is for a complete replacement of the Database API.

# Background

The current Data


# Problems with the Current Database API

## The current interface is poorly defined

There is *high-level/low-level* split in the Database driver and it should be
possible to define a new low-level driver that uses a non-PostgreSQL backend.
However, in
practice the responsibilities of each level is not clearly defined,
and with only one low-level backend we are not sure how leaky this abstraction is.

## Too much complexity is pushed into Database Driver

- Lineage traversal and verification
- *Dataset* to *Product* auto-matching

### Metadata Indirection

*Metadata indirection* is of particular concern as far as alternative backend
implementations go. User defined search fields with custom aggregate
functions are way too complex. It is essentially a JSON DSL that is compiled into
*SQLAlchemy* expressions, too hard to re-implement, and might not even be feasible
for
simpler databases like SQLite. This is an example from current EO metadata
spec:

```yaml
        lat:
            description: Latitude range
            type: float-range
            max_offset:
            - [extent, coord, ur, lat]
            - [extent, coord, lr, lat]
            - [extent, coord, ul, lat]
            - [extent, coord, ll, lat]
            min_offset:
            - [extent, coord, ur, lat]
            - [extent, coord, lr, lat]
            - [extent, coord, ul, lat]
            - [extent, coord, ll, lat]
```

Which means something like this:

- Define search field named `lat` which is of type `Range<float>`
  - where range starts at: `min(data.extent.coord.ur.lat as float, data.extent.coord.lr.lat as float, ...)`
  - and stops at:  `max(data.extent.coord.ur.lat as float, data.extent.coord.lr.lat as float, ...)`

This then gets compiled into a sql expression when queries are made.

## Lack of Spatial Constructs in the Interface

As far as database driver goes spatial information is no different than any
other user defined metadata (sort of, lat|lot|time are treated specially as far
as index creation goes). Core code understands spatial queries, but before db
driver can see it, spatial query gets converted into lat/lon range query. This
effectively means that datasets and queries can not cross problematic lines like
antimeridian.

This also leads to data duplication, metadata document has to include lat/lon
representation of datasets footprint as well as the native footprint. This
[report](https://s3-ap-southeast-2.amazonaws.com/ga-aws-dea-dev-users/u60936/Datacube-Spatial-Query-Problem.html)
highlights how this data duplication leads to incorrect spatial index
implementation.

## Confusing Auto Matching Behaviour due to Lineage Handling

When adding datasets to the database there is no simple way to specify which
product these datasets ought to belong to, instead datasets need to be
*auto*-matched to products. The reason why auto-matching is necessary in the
current implementation is due to the handling of lineage. A single dataset
document might include lineage datasets, so any addition of a single dataset
to the index could actually result in the addition of multiple datasets to
multiple products.

Users often assume that auto-matching relies on band names, and are surprised
when auto-matching fails despite a product having the same band names as the
dataset being indexed. Band names **are** used during the auto-matching
process, but only to accept or reject an already matched *Product* (through
the `product.metadata`). When `product.metadata` is not sufficiently
unique, auto matching breaks down even when band names would be sufficiently
unique constraint to match with. This is a recurring pain point encounted by many people.

Auto-matching used to happen inside the database driver. This has been changed,
but it is still tightly coupled to the driver. The same applies for lineage traversal.


## Other Database and Metadata Structure Issues

- Whole dataset or nothing interface
  - no easy way to query just spatial data
  - or just lineage data
  - or just data needed for IO
  - Large volume of data pushed between db and core
- Broken streaming [issue 542](https://github.com/opendatacube/datacube-core/issues/542)
  - particularly painful for full-history queries (20+ minute wait and several
    Gigabytes of RAM used before first dataset pops out of `find_datasets_lazy`
    is not cool)
- Incomplete lineage access API
  - ~~Can get parents but not children~~
  - Can not store lineage information for *external datasets*
  - Can not reference an existing lineage dataset, include entire
    metadata document of the lineage dataset
- Way too immutable
  - Limited ability to change indexed data in place
  - Not possible to delete or rename *Products* or *Metadata Types*
  - Inconsistent mutability: can change dataset location for the whole dataset,
    but can not change locations of individual bands
- Lack of extent information querying
  - No way to ask for valid time range
  - No way to ask for valid spatial range
- No tracking of important per-band metadata such as:
  - Image dimensions
  - Per band spatial information
  - This limits native load functionality that can be implemented efficiently

# Proposed Solution

## Principles

### Spatial Data is Special and so is Time

Model spatial and time extents explicitly.

- When persisting dataset to database communicate per band footprints to db layer
- When querying, pass CRS+Polygon of the query directly to db layer, rather than just lat/lon ranges

This would allow implementations that have native CRS other than lat/lon. If one
needs to maintain a catalog of datasets in "challenging" regions like
Antarctica, it would be possible: store dataset extents in EPSG:3031, which
should not have discontinuities in the region of interest, and define/translate your
queries into same CRS as well.

Database layer can now keep track of spatial/temporal extents, know that it is
aware of them explicitly.

We need better model for time querying, that takes into account time zone of the
data. When user asks for everything on 1-May-2019 they don't mean UTC
1-May-2019, they mean 1-May-2019 in the locality of the spatial query.

### Simplify/drop custom search fields

Use STAC-like properties for custom querying. These are key value pairs, where
keys are strings and values are limited to "simple" data types: number, datetime,
string (full set of types is to be defined). No nesting, no arrays, no ranges,
no aggregation.

Properties intended for querying are defined and stored separately from the rest
of the dataset document.

Arbitrary nested user data can be stored and retrieved, but ability to query it
is not guaranteed. DB back-end might provide querying of arbitrary sub-trees,
similar to current search fields, with the exception that aggregation is not
going to be supported. So you can dynamically create query equivalent to:

```
Only return datasets that have a numeric field at
 `user_data.algorithm.runtime.blob_count`
with value less than `10`
```

but not query with aggregations like min/max/avg. Such ad-hoc querying will be
optional, database backend implementations might choose to not to support it.

This means we won't need what is currently called "MetadataType". Database layer
will only be concerned with Products, Datasets and maybe Measurements.

### Drop "sources" query

Currently it is possible to filter datasets based on properties of their
parents. The complexity of generated query is really high, cost of running such
query is very high. The complexity of the code that generates such query is
also really high. What is worse this "sources query" does not generalize to
datasets that have multiple identical parents, i.e. any stats datasets.

It is more efficient to simply copy important properties (cloud cover, gqa,
region code, etc.) from parent dataset to derived dataset, and only when it
makes sense, than rely on lineage tree traversal as part of the query, that will
always be a costly operation. There is no meaningful "gqa" for an annual
geomedian dataset, but it makes perfect sense to copy "gqa" for FC or WoFS
datasets.


### Changes to Auto Matching and Lineage

Change current rule:

```
Dataset can have more bands than product it matches to,
so long as it has all the bands defined in the product.
```

to be simply:

```
Dataset should have exactly the same bands as product it is matching to.
```

Only when there are products with exactly the same set of band names extra
differentiation mechanism will be necessary.


### Performance Related API Changes

- Bulk add of datasets, indexing is slow primarily because we add "one" dataset
  at a time (really we add several at a time, because lineage, but we can't add
  several top level datasets at once)
- Proper streaming of datasets, fast to start with low memory overhead
- Support efficient streaming of datasets ordered by time
  - Arbitrary order of datasets within a stream means that one can not match up
    datasets from two different products without reading all datasets into
    memory first
  - I would not try and make it generic "order by some property", just time.
  - With time being an explicit and required dataset property, DB driver should
    be able to arrange for storage that allows efficient ordered extraction of
    datasets. It is difficult with the current system because (a) time is a
    range not a timestamp (b) time is indirected via metadata_type mechanism, and is
    not even guaranteed to be present.


# Preliminary API Notes

## Persist/Data Model

### Products

As far as database layer goes products are just named groupings of datasets.
Product has a name and a definition document. Content of the product definition
document is just an opaque json blob not interpreted by the database layer.

Unlike current system there is no "metadata".

TODO: maybe measurements should be understood by db layer, at least names of bands.

### Datasets

Non optional fields are:

1. `id` UUID of the dataset
2. `product` name of the product this dataset belongs to
3. `time` UTC timestamp or a tuple of timestamps (start, end)
   - Single timestamp means single instance in time
   - Support open ranges like: `(None, t)`, `(t, None)` to be equivalent of
     `(-Inf, t]` and `[t, +Inf)` respectively

Optional explicit fields:

1. Location: URI base relative to which band paths are resolved. For single file
   datasets (netcdf/hdf5/sometimes tiff), location should point to the file
   containing all the bands.

2. Geospatial: CRS+Grids+(optional: Valid data polygon)
   - Assume common CRS across all bands
   - Possibly different resolutions/pixel alignments per band
   - Bands with common geometry are grouped together and this group is given a
     name (e.g. `'default'`, `'20m'`)
   - There must be a grid named `'default'`
   - For each group `Pixel Grid` is recorded:
     - `shape` -- `(ny, nx)` number of pixels along `y` and `x` directions
     - `affine` -- Linear mapping (affine matrix) from "pixel space" to "world
       space" as defined by CRS.
   - Optionally one can supply a tighter bound based on valid pixel data when
     available. Geometry is defined within the same CRS as pixel grids and
     should contain no valid pixels of any band outside of the specified
     polygon.
   - CRS, Affine and shape fully define footprint of each band as well as
     resolution. It is up to db layer to incorporate those footprints into
     spatial index of sort.
   - While CRS+Affine is a common mechanism for geo-registration, it is not the
     only one. Some collections use Ground Control Points for geo-registration.
     This is not something we support currently, but probably worth thinking
     about this use case. For now one can compute approximate Affine matrix from
     all GCPs.

3. Bands, dictionary of per band information
   - `path` - relative path of the file containing raster of this dataset (path
     is allowed to be absolute, but prefer relative paths, can also be empty for
     in which case location of the dataset is used).
   - `band: int` 1-based band index inside the file `band=1` is assumed when missing
   - `layer: str` name of the variable within the file (netcdf variable name, hdf5 path) (optional)
   - `grid: str` name of the pixel grid (optional, defaults to `'default'`)
   - `driver_data: dict` opaque json blob of arbitrary IO driver data, this data
     is passed on without interpretation from db layer to IO layer (optional)

4. Properties, STAC like key value store
   - Keys are namespaced strings, prefer standard STAC names where possible
   - Namespace separator is `:` (STAC convention)
   - Values are simple types only: string, number, datetime, no arrays, no dictionaries
   - Database layer should support querying these fields, essentially this is a
     simplification of the current `metadata.search_fields` mechanism.

5. Lineage data
   - List of `(uuid: UUID, label:str)` tuples defining direct predecessors of the dataset
   - Unlike current system there is no uniqueness constraint on the label, no
     need to use `nbar{0,1,..}`, just use `nbar` label on all nbar inputs.
   - Lineage data is just a reference, no repetition of the lineage dataset
     metadata, and hence no need to verify consistency at this level, can be
     done by core for any db backend.
   - Lineage datasets ought to be already present in the database or need to be
     added together in the same bulk add.

6. IO Driver data
   - Arbitrary json blob to be passed to IO driver, this is "per whole dataset"
     data, per band data should go into individual bands `driver_data` section.

7. User data
   - Arbitrary json blob not interpreted by datacube-core in any form

Persist API should support bulk addition of datasets, this is necessary to
support efficient back-end implementations.


## Retrieve

### Product

- Extract full list of product names
- Extract product definition given product name

### Dataset

- Should be able to retrieve parts of the dataset without extracting all the
  information
  - For IO tasks: GeoSpatial data, subset of bands, io driver data
  - For Data Extent Visualisation: GeoSpatial data
  - For interactive inspection: all the data
- Support bulk retrieve for efficient db comms
- Should be able to stream dataset data for a given product with small startup
  latency and low memory overhead
- Streaming should support ordering by time. This is to support multi-product
  processing without having to resort to loading all datasets into memory first.

## Data Summaries

DB layer is expected to provide summary data for a given product

- Total count of datasets
- Total count of "live" (not archived) datasets
- Temporal extents
- Spatial extents, this can be maintained in any CRS, not limited to EPSG:4326

## Query

Query is only applied to datasets, no searching of products, this will be
implemented in core code if needed. Assumption is that product list is small
enough to fit into RAM without any problem and so any product filtering can be
implemented in python code operating on product definition documents.

Datasets are selected based on:

- Product name
- Time bounds of a query
- Spatial extents of query
  - Spatial query can be defined by an arbitrary polygon in any CRS, not limited
    to lat/lon bounds.
- Dataset properties

API should support:

- Streaming of query results
- Streaming ordered by time
- Streaming of parts of dataset metadata
  - Least data: just UUIDs of datasets matching query
  - Most data: All available information including lineage/offspring UUIDs
- Should be consistent with retrieve API

# Appendix

- https://github.com/opendatacube/dea-proto/blob/master/docs/change-dataset-metadata.md
- https://github.com/orgs/opendatacube/teams/developers/discussions/5
- https://github.com/opendatacube/dea-proto/blob/master/docs/normalised-dataset.yaml