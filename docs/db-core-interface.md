# ODC Enhancement: Replace the ODC Index and API

**Date:** 2019-06-12

**Author:** Kirill Kouzoubov  (@Kirill888), Damien Ayers (@omad), Alex Leith (@alexgleith)

**Version:** datacube-core >=  1.7

# Introduction

The Database API is used in the ODC Core for all access to the ODC Index. The Index
is intended to be very flexible in the metadata formats it supports, while also
maintaining high performance searching across multiple dimensions.

Mostly, it has been successful in these goals. Unfortunately, while trying to
implement improvements like fast extents queries (link), native resolution data
loading (insert link) and other smaller changes, we have run up against several
assumptions and design flaws which are limiting future ODC development.

This proposal is for a replacement of the ODC Index and Database API and
a change in the internal model of *Datasets*.

## Summary

Instead of storing heterogeneous dataset structures in the database with a lot of
flexibility in what is indexed (in a database index sense), a new database
structure with required fields stored as database columns, and only ancilliary
data in the dynamic `json` metadata column will be implemented. This will result
in simpler, more maintainable code in the ODC Core, much simpler indexes and easier and likely faster
querying.

Key changes suggested are documented in detail below, but include the following:

- Add required *geometry* field(s) with defined CRS
- Add required *temporal* field(s) with defined time zone
- Remove custom indexing and replace with standard indices
- Change auto-matching to require exact match of bands
- Implement methods for bulk-insertion of datasets and streaming of dataset listings.

# Problems with the Database API

## The interface is poorly defined

There is *high-level/low-level* split in the Database driver and it should be
possible to define a new low-level driver that uses a non-PostgreSQL backend.
However, the responsibily of each level is not clearly defined,
and with only one low-level backend, we suspect that this is not a clear split.

## Too much complexity in the driver

- *Dataset* to *Product* auto-matching (Metadata indirection)
- Lineage traversal and verification

### Metadata indirection (Metadata Types)

*Metadata indirection* is a particularly difficult feature for any future
database backends to implement. This feature allows users to define new search
fields over *dataset documents*, potentially by aggregating multiple portions of a dataset together using custom aggregation functions.

It is a JSON [DSL] that is compiled into *SQLAlchemy*
expressions, and then run as PostgreSQL specific database queries. This would be very hard to re-implement in another database, and might not even be possible for
simpler databases like SQLite.

This is an example from the EO metadata spec:


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

Broken down, this means something like:

- Define a search field named `lat` which is of type `Range<float>`
  - where range starts at: `min(data.extent.coord.ur.lat as float, data.extent.coord.lr.lat as float, ...)`
  - and stops at:  `max(data.extent.coord.ur.lat as float, data.extent.coord.lr.lat as float, ...)`

This is then compiled into an SQL expression to generate table indexes per
product, and an identical expression when queries are made.

## Lack of Spatial Constructs in the Interface

In the current ODC, as far as the database driver is concerned, spatial
information is no different than any other user defined metadata. (sort of,
lat|lot|time are treated specially as far as index creation goes). Core code
understands spatial queries, but before db driver can see it, spatial query
gets converted into lat/lon range query. This effectively means that datasets
and queries can not cross problematic lines like antimeridian.

This also leads to data duplication, a valid dataset document has to include lat/lon
representation of a datasets' footprint as well as the native footprint. This
[report](https://s3-ap-southeast-2.amazonaws.com/ga-aws-dea-dev-users/u60936/Datacube-Spatial-Query-Problem.html)
highlights how this data duplication has led to incorrect spatial indexing of
many datasets.

## Confusing Auto Matching Behaviour due to Lineage Handling

When adding *Datasets* to the database there is no simple way to specify which
*Product* these datasets ought to belong to. Instead datasets need to be
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

- Poor support for partial dataset querying:
    - no easy way to query just spatial data
    - or just lineage data
    - or just data needed for IO
    - Which often leads to a large volume of data pushed between the Database and
      core
- Broken streaming [issue 542](https://github.com/opendatacube/datacube-core/issues/542)
    - particularly painful for full-history queries (20+ minute wait and several
    Gigabytes of RAM used before first dataset pops out of `find_datasets_lazy`)
- Incomplete lineage access API
    - Can not store lineage information for *external datasets*
    - No way to reference an existing lineage dataset. It's necessary to include
      the entire metadata document of the parent datasets.
- Too much immutability
    - Limited ability to change indexed data in place
    - Not possible to delete or rename *Products* or *Metadata Types*
    - Inconsistent mutability: eg. it's possible to change the location for
      a while dataset, but not change locations of individual bands
    - This is really frustrating for anyone learning or experimenting with ODC.
      If you index something with the wrong name, you can either drop the
      database and start again, or resort to fragile hand written SQL.
- Lack of extent information querying
    - There is no way to ask for the full valid time range
    - There is no way to ask for the full valid spatial range
- No tracking of important per-band metadata such as:
    - Image dimensions
    - Per band spatial information
    - This limits native load functionality that can be implemented efficiently

# Proposed Solution

## Principles

### Spatial Data is Special and so is Time

Model spatial and time extents explicitly.

- When persisting datasets to the database communicate per band footprints to
  the Database layer
- When querying, pass CRS+Polygon of the query directly to the Database layer,
  rather than just lat/lon ranges

This would allow implementations that have a native CRS other than lat/lon. If
one needs to maintain a catalog of datasets in *challenging* regions like
Antarctica, it would be possible: store dataset extents in EPSG:3031, which
should not have discontinuities in the region of interest, and define/translate
your queries into the same CRS as well.

The Database layer can now keep track of spatial/temporal extents, now that it is
aware of them explicitly.

We need a better model for time querying, that takes into account the time zone
of the data. When a user asks for everything on 1-May-2019 they don't mean UTC
1-May-2019, they mean 1-May-2019 in the locality of the spatial query.

### Simplify/drop custom search fields

Use STAC-like properties for custom querying. These are key value pairs, where
keys are strings and values are limited to "simple" data types: number, datetime,
string (full set of types is to be defined). No nesting, no arrays, no ranges,
no aggregation.

Properties intended for querying are defined and stored separately from the rest
of the dataset document.

Arbitrarily nested user data can be stored and retrieved, but the ability to
query it is not guaranteed. A Database back-end might provide querying of
arbitrary sub-trees, similar to the current search fields, with the exception that
aggregation is not going to be supported. So you can dynamically create a query
equivalent to:

```
Only return datasets that have a numeric field at
 `user_data.algorithm.runtime.blob_count`
with value less than `10`
```

but not query with aggregations like `min`/`max`/`avg`. Such ad-hoc querying
will be optional, database backend implementations could choose to not to
support it.

This means we will no longer need *MetadataType*s. The Database
layer will only contain Products, Datasets and maybe Measurements.

### Drop *sources*/*lineage* querying

It is possible to filter datasets based on properties of their
parents. The complexity of these generated queries, the cost of running them, much less the complexity of the code generating them is all really high.
What is worse, this *sources query* does not generalize to
datasets that have multiple parents, ie. any stats datasets.

It is more efficient to simply copy important properties (eg. cloud cover,
geometric quality, region code) from parent datasets to derived dataset, but to
only do so when it makes sense. Many derived products combine input datasets in
such a way as to make these sorts of queries meaningless.  There is no
meaningful *geometric quality* for an annual geomedian dataset, but it does make
sense to copy *geometric quality* for Fractional Cover or Water
Observation datasets.


### Changes to Auto Matching and Lineage

Change the rule (implemented currently):

> A Dataset must contain all the bands defined in the Product it matches to,
  but can contain more bands.

to be simply:

> A Dataset should have exactly the same bands as the Product it matches to.

Only when there are Products with exactly the same set of band names will extra
differentiation mechanisms be necessary.


### Performance Related API Changes

- Bulk add of datasets, indexing is slow primarily because we add *one* dataset
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
      range not a timestamp (b) time is indirected via metadata_type mechanism,
      and is not even guaranteed to be present.


# Appendix

- https://github.com/opendatacube/dea-proto/blob/master/docs/change-dataset-metadata.md
- https://github.com/orgs/opendatacube/teams/developers/discussions/5
- https://github.com/opendatacube/dea-proto/blob/master/docs/normalised-dataset.yaml


[DSL]: https://en.wikipedia.org/wiki/Domain-specific_language
