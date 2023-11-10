[![Test Status](https://github.com/opendatacube/odc-tools/actions/workflows/main.yml/badge.svg)](https://github.com/opendatacube/odc-tools/actions/workflows/main.yml)
[![codecov](https://codecov.io/gh/opendatacube/odc-tools/branch/develop/graph/badge.svg?token=PovpVLRFwn)](https://codecov.io/gh/opendatacube/odc-tools)

DEA Prototype Code
==================

This repository provides developmental [libraries](https://github.com/opendatacube/odc-tools/tree/develop/libs)
and [CLI tools](https://github.com/opendatacube/odc-tools/tree/develop/apps) for Open Datacube.

- AWS S3 tools
- CLIs for using ODC data from AWS S3 and SQS 
- Utilities for data visualizations in notebooks
- Experiments on optimising Rasterio usage on AWS S3 

Full list of libraries, and install instructions:

- `odc.ui` tools for data visualization in notebook/lab
- `odc.io` common IO utilities, used by apps mainly
- `odc-cloud[ASYNC,AZURE,THREDDS]` cloud crawling support package
  - `odc.aws` AWS/S3 utilities, used by apps mainly
  - `odc.aio` faster concurrent fetching from S3 with async, used by apps `odc-cloud[ASYNC]`
  - `odc.{thredds,azure}` internal libs for cloud IO `odc-cloud[THREDDS,AZURE]`

## Promoted to their own repositories 
- `odc.stats` large scale processing framework (Moved to [odc-stats](http://github.com/opendatacube/odc-stats))
- `odc.stac` STAC to ODC conversion tools (Moved to [odc-stac](https://github.com/opendatacube/odc-stac))
- `odc.dscache` experimental key-value store where `key=UUID`, `value=Dataset` (moved to [odc-dscache](https://github.com/opendatacube/odc-dscache))

Installation
============

Libraries and applications in this repository are published to PyPI, and can be installed \
with `pip` like so:

```
pip install \
  odc-ui \
  odc-stac \
  odc-stats \
  odc-io \
  odc-cloud[ASYNC] \
  odc-dscache
```

For Conda Users
---------------

Some **odc-tools** are available via `conda` from the `conda-forge` channel.


```
conda install -c conda-forge odc-apps-dc-tools odc-io odc-cloud 

```


Cloud Tools
===========

Installation
------------

Cloud tools depend on the `aiobotocore` package, which depends on specific
versions of `botocore`. Another package we use, `boto3`, also depends on
specific versions of `botocore`. As a result, having both `aiobotocore` and
`boto3` in one environment can be a bit tricky. The way to solve this
is to install `aiobotocore[awscli,boto3]` before anything else, which will install
compatible versions of `boto3` and `awscli` into the environment.

```
pip install -U "aiobotocore[awscli,boto3]==1.3.3"
# OR for conda setups
conda install "aiobotocore==1.3.3" boto3 awscli
```


1. For cloud (AWS only)
   ```
   pip install odc-apps-cloud
   ```
2. For cloud (GCP, THREDDS and AWS)
   ```
   pip install odc-apps-cloud[GCP,THREDDS]
   ```
2. For `dc-index-from-tar` (indexing to datacube from tar archive)
   ```
   pip install odc-apps-dc-tools
   ```

Apps
----

1. `s3-find` list S3 bucket with wildcard
2. `s3-to-tar` fetch documents from S3 and dump them to a tar archive
3. `gs-to-tar` search GS for documents and dump them to a tar archive
4. `dc-index-from-tar` read yaml documents from a tar archive and add them to datacube


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


Local Development
=================

The following steps are used in the GitHub Actions workflow `main.yml`

```bash

# build environment from file
mamba env create -f tests/test-env-py39.yml

# this environment name is defined in tests/test-env-py38.yml file
conda activate odc-tests-py38

# install additional packages
./scripts/dev-install.sh --no-deps

# setup database for testing
./scripts/setup-test-db.sh

# run test
echo "Running Tests"
pytest --cov=. \
--cov-report=html \
--cov-report=xml:coverage.xml \
--timeout=30 \
libs apps

# Optional, to delete the environment
conda env remove -n odc-tests-py38
```

Use `conda env update -f <file>` to install all needed dependencies for
`odc-tools` libraries and apps.

<details><summary>Conda `environment.yaml` (click to expand)</summary><div markdown="1">

```yaml
channels:
  - conda-forge
dependencies:
  # Datacube
  - datacube>=1.8.5

  # odc.dscache
  - python-lmdb
  - zstandard

  # odc.ui
  - ipywidgets
  - ipyleaflet
  - tqdm

  # odc-apps-dc-tools
  - pystac>=1
  - pystac-client>=0.2.0
  - azure-storage-blob
  - fsspec
  - lxml  # needed for thredds-crawler

  # odc.{aio,aws}: aiobotocore/boto3
  #  pin aiobotocore for easier resolution of dependencies
  - aiobotocore==1.3.3
  - boto3

  # eodatasets3 (used by odc-stats)
  - boltons
  - ciso8601
  - python-rapidjson
  - requests-cache
  - ruamel.yaml
  - structlog
  - url-normalize

  # for dev
  - pylint
  - autopep8
  - flake8
  - isort
  - black
  - mypy

  # For tests
  - pytest
  - pytest-httpserver
  - pytest-cov
  - pytest-timeout
  - moto
  - deepdiff

  - pip>=20
  - pip:
      # odc.apps.dc-tools
      - thredds-crawler

      # odc.stats
      - eodatasets3

      # tests
      - pytest-depends

      # odc.ui
      - jupyter-ui-poll

      # odc-tools libs
      - odc-stac
      - odc-ui
      - odc-dscache
      - odc-stats

      # odc-tools CLI apps
      - odc-apps-cloud
      - odc-apps-dc-tools
```
</div></details>

Release Process
===============

1. Manually edit `{lib,app}/{pkg}/odc/{pkg}/_version.py` file to increase version number
2. Merge changes to the `develop` branch via a Pull Request
3. Fast-forward the `pypi/publish` branch to match `develop`
4. Push to GitHub

Steps 3 and 4 can be done by an authorized user with
`./scripts/sync-publish-branch.sh` script.


Publishing to [PyPi](https://pypi.org/) happens automatically when changes are
pushed to the protected `pypi/publish` branch. Only members of [Open Datacube
Admins](https://github.com/orgs/opendatacube/teams/admins) group have the
permission to push to this branch.
