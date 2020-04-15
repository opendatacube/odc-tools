[![Build Status](https://github.com/opendatacube/odc-tools/workflows/build/badge.svg)](https://github.com/opendatacube/odc-tools/actions)

DEA Prototype Code
==================

- AWS s3 tools
- Rasterio from S3 investigations
- Utilities for data visualizations in notebooks

Installation
============

This repository provides a number of small [libraries](https://github.com/opendatacube/odc-tools/tree/master/libs)
and [CLI tools](https://github.com/opendatacube/odc-tools/tree/master/apps).

Full list of libraries, and install instructions:

- `odc.ui` tools for data visualization in notebook/lab
- `odc.index` extra utils for working with datacube database
- `odc.geom` geometry utils and prototypes
- `odc.algo` algorithms (GeoMedian wrapper is here)
- `odc.io` common IO utilities, used by apps mainly
- `odc.aws` AWS/S3 utilities, used by apps mainly
- `odc.aio` faster concurrent fetching from S3 with async, used by apps
- `odc.dscache` experimental key-value store where `key=UUID`, `value=Dataset`
- `odc.dtools` tools/experiments in the area of dask.distributed/dask<>datacube integration
- `odc.ppt` parallel processing helper methods, internal lib

Installation requires using custom package repo `https://packages.dea.ga.gov.au`.

```
pip install --extra-index-url="https://packages.dea.ga.gov.au" \
  odc-ui \
  odc-index \
  odc-geom \
  odc-algo \
  odc-io \
  odc-aws \
  odc-aio \
  odc-dscache \
  odc-dtools
```

**NOTE**: on Ubuntu 18.04 default `pip` version is awfully old and does not
support `--extra-index-url` command line option, so make sure to upgrade `pip`
first: `pip3 install --upgrade pip`.


CLI Tools
=========

Installation
------------


1. For cloud (AWS only)
   ```
   pip install --extra-index-url="https://packages.dea.ga.gov.au" odc-apps-cloud
   ```
2. For cloud (GCP, THREDDS and AWS)
   ```
   pip install --extra-index-url="https://packages.dea.ga.gov.au" 'odc-apps-cloud[GCP,THREDDS]'
   ```
2. For `dc-index-from-tar` (indexing to datacube from tar archive)
   ```
   pip install --extra-index-url="https://packages.dea.ga.gov.au" odc-apps-dc-tools
   ```

NOTE: cloud tools depend on `aiobotocore` which has a dependency on a specific
version of `botocore`, `boto3` also depends on a specific version of `botocore`
as a result having both `aiobotocore` and `boto3` in one environment can be a bit
tricky. The easiest way to solve this is to install `aiobotocore[awscli,boto3]` before
anything else, which will pull in a compatible version of `boto3` and `awscli` into the
environment.

```
pip install -U 'aiobotocore[awscli,boto3]'
```

Apps
----

1. `s3-find` list S3 bucket with wildcard
2. `s3-to-tar` fetch documents from S3 and dump them to tar archive
3. `gs-to-tar` search GS for documents and dump them to tar archive
4. `dc-index-from-tar` read yaml documents from tar archive and add them to datacube


Example:

```bash
#!/bin/bash

s3_src='s3://dea-public-data/L2/sentinel-2-nrt/**/*.yaml'

s3-find "${s3_src}" | \
  s3-to-tar | \
    dc-index-from-tar --env s2 --ignore-lineage
```

Fastest way to list regularly placed files is to use fixed depth listing:

```bash
#!/bin/bash

# only works when your metadata is same depth and has fixed file name
s3_src='s3://dea-public-data/L2/sentinel-2-nrt/S2MSIARD/*/*/ARD-METADATA.yaml'

s3-find --skip-check "${s3_src}" | \
  s3-to-tar | \
    dc-index-from-tar --env s2 --ignore-lineage
```

When using Google Storage:

```bash
#!/bin/bash

# Google Storage support
gs-to-tar --bucket data.deadev.com --prefix mangrove_cover
dc-index-from-tar --protocol gs --env mangroves --ignore-lineage metadata.tar.gz
```
