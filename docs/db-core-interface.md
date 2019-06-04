# Introduction

**This is an evolving document**

- Why do we need to change DB layer
- How we gonna change it

# Problems with the Current State

## Interface is poorly defined

Supposedly there is high-level|low-level split in the DB driver and it should be
possible to define a new low-level driver that uses on non-pg backend, in
practice the responsibility of hi|lo levels is not well understood/articulated,
and with only one low-level backend we are not sure how leaky this boundary is.

## Too much complexity pushed into DB driver

- Lineage traversal/verification
- Dataset to product auto-matching
- The whole metadata indirection mechanism

"Metadata indirection" is of particular concern as far as alternative backend
implementations go. User defined search fields with custom aggregate functions
are way too complex. It is essentially a json DSL that gets compiled into
SqlAlchemy expressions, too hard to re-implement, might not even be possible for
simpler db backends like sqlite.

This is an example from current EO metadata spec:

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

## Other

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
  - Can get parents but not children
- Way too immutable
  - Limited ability to change indexed data in place
  - No mechanisms to delete/rename products/metadata definitions
  - Inconsistent mutability: can change dataset location for the whole dataset,
    but can not change locations of individual bands
- Lack of extent information querying
  - No way to ask for valid time range
  - No way to ask for valid spatial range
- DB not tracking important per-band metadata
  - Image dimensions
  - Per band spatial information
  - This limits native load functionality that can be implemented efficiently

# Proposed Interface

## Main Principles

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
 `user_data.algorithm.parameters.blob_count`
with value less than `10`
```

but not query with aggregations like min/max/avg. Such ad-hoc querying will be
optional, database backend implementations might choose to not to support it.

### Drop "sources" query

Currently it is possible to filter datasets based on properties of their
parents. The complexity of generated query is really high, cost of running such
query is very high. The complexity of the code that generates such query is
also really high. What is worse this "sources query" does not generalize to
datasets that have multiple identical parents, i.e. any stats datasets.

It is more efficient to simply copy important properties (cloud cover, gqa,
region code, etc.) from parent dataset to derived dataset, when it makes sense,
than rely on lineage tree traversal as part of the query, that will always be a
costly operation. There is no meaningful "gqa" for an annual geomedian dataset,
but it makes perfect sense to copy "gqa" for FC or WoFS datasets.


# Appendix

- https://github.com/opendatacube/dea-proto/blob/master/docs/change-dataset-metadata.md
- https://github.com/orgs/opendatacube/teams/developers/discussions/5
- https://github.com/opendatacube/dea-proto/blob/master/docs/normalised-dataset.yaml
