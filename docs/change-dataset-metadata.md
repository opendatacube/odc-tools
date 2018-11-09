# Abstract

I have [documented
previously](https://github.com/orgs/opendatacube/teams/developers/discussions/5)
problems with the current approach for handling spatial metadata. If you haven't
read this already I strongly encourage you to do so before proceeding. Changes I
proposed there don't address the issue of not recording enough information for
"native loads". So I have a more comprehensive change proposal that addresses
correctness issues as well as issue of "native loads".


# Changes

There are two major axis to this work: metadata format changes, datacube changes
to support the change.

1. What information is supplied by the user via dataset metadata documents
  - Supply only raw spatial metadata about dataset bands
  - Even that can be made optional as this metadata can be fetched from the files
    during indexing, while this will be slower, some users, particularly those
    working with smaller data volumes, might prefer this approach

2. How this information is handled by Datacube
  - Compute spatial extents correctly from supplied per band metadata during
    indexing step (rather than during "prepare step"), or even fetch from files
    if needed
  - Store spatial extent separately from the metadata document (json blob)

Advantages:

- Remove code duplication: extent computation happens in one place, not across
  tens of different prepare scripts
- No need to fix all prepare scripts and document complex steps that need to be
  taken to compute extents correctly
- Opens up possibility for handling non-trivial cases like date-line/poles in the future
- By moving spatial metadata out of the json blob we can keep the assumption
  that metadata document stored in DB is "roughly the same" as on disk, while
  still allowing spatial metadata computation at index time, instead of
  "prepare" time


## Spatial data per band

An individual dataset is composed of multiple bands observed roughly at the same
time. Each band can have a slightly different spatial footprint, and can have
different resolutions. Following triplet fully captures spatial
information about pixel plane and it's mapping to the Earth's surface:

1. Coordinate Reference System (CRS) -- defines a mapping from geodetic
   coordinates to a "plane" approximation.

2. Transform -- a linear mapping (Affine transform) between pixel plane
   coordinates and coordinates of a plane defined by CRS (pixel <> meters)

3. Image size -- defines a region covered by a given observation in pixel plane
   `(0,0) -> (W,H)`, this can then be transformed into a rectangle in a CRS plane,
   and from there to some non-trivial shape on a sphere.

Right now we assume that CRS is the same for all bands within a dataset, we can
keep this assumption.

Currently datacube does not keep track of image size(3) and transform(2),
instead we only store derived information -- a bounding box of a dataset
footprint (computed by prepare scripts, incorrectly, and with an assumption that
dataset isn't in any of the challenging regions like poles or across date-line).

The consequence of not keeping information about pixel plane are:

- Native load is harder to implement
  - Have to open individual band files before deciding how much memory to allocate for pixels
- Can not predict load costs ahead of time
- Can not report basic stats about dataset without opening files first


## Move spatial data out of json blob

Current assumptions:

- Spatial information is part of json blob stored in DB and is not really
  "special", only semi-special (because of time-lat-lon indexes)

- Dataset on disk is the same as dataset in DB (apart for some data loss due to
  YAML->json conversion)

The corollary of these two assumptions is:

- Spatial extents have to be supplied from outside ("prepare" time), because we
  can not inject "computed" values in to the document, and we have nowhere else
  to put it.

To have spatial index data computed at index time we have to break one of these
assumptions. I think it's best to move spatial data into a separate column/table
than to combine user-supplied and derived data under the same json blob. This
might have performance benefit for spatial querying and opens up possibility to
experiment with GIS indexes.

I propose we keep the following data in some place other than json blob:

- Bounding box of the dataset in Lon/Lat space
- Pixel grid definitions per band (CRS, Transform, WxH)
- Temporal extents of a dataset (for ease of combining spatial and temporal indexes)
- Valid data region (a more precise version of dataset footprint that takes
  pixel values into account)

It's important to keep Pixel grid definitions separate from the json blob to
allow computing that information from files directly (user only need to supply
paths to image files, nothing else). Same with valid data region, if it is
supplied by prepare script copy it out, if not supplied, having it stored
separately will allow us to compute it if requested.

## Metadata format

Basic idea is to supply CRS, shape and transform fields per band, to avoid
duplication allow a default set with per band overrides as necessary. Below is
an example for Lansat 8 scene. The "default set" should always be present, so
following keys are compulsory: `extent.crs`, `extent.shape` and
`extent.transform`.

```yaml
extent:
  # time could be a single timestamp or a range: [t_start, t_end]
  time: "2018-09-23T00:40:47.7523390Z"

  # Defines default Pixel Grid for most bands
  #  crs: prefer EPSG when possible, but can be WKT
  #  shape: Height, Width  (same as rasterio/ndarray)
  #  transform: same as transform in `rio info`,
  #             9 values: row major representation of an Affine matrix (3x3)
  #             mapping from pixel plane to a plane defined by CRS
  #             last three values 0,0,1
  crs: "EPSG:32654"
  shape: [7731, 7621]
  transform: [30.0, 0.0, 306285.0, 0.0, -30.0, -1802085.0, 0, 0, 1]

  # Optional GeoJSON object defining valid region of the dataset in the plane
  # defined by CRS.
  # 
  # Valid region partitions space into two:
  #   Outside -- has no valid data at all
  #   Inside  -- has all the valid data, but can have some invalid data
  valid_region: {..GeoJSON..}

  # Here you can overwrite bands that are special, for those datasets that have
  # multiple resolutions, or pixel grids that don't align across bands.
  bands:
    panchromatic:
      shape: [15461, 15241]
      transform: [15, 0, 306292.5, 0, -15, -1802092.5, 0, 0, 1]
```


# Links

- https://s3-ap-southeast-2.amazonaws.com/ga-aws-dea-dev-users/u60936/Datacube-Spatial-Query-Problem.html
- https://github.com/orgs/opendatacube/teams/developers/discussions/5
