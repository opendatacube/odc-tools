[![Test Status](https://github.com/opendatacube/odc-tools/actions/workflows/main.yml/badge.svg)](https://github.com/opendatacube/odc-tools/actions/workflows/main.yml)
[![codecov](https://codecov.io/gh/opendatacube/odc-tools/branch/develop/graph/badge.svg?token=PovpVLRFwn)](https://codecov.io/gh/opendatacube/odc-tools)

DEA Prototype Code
==================

- AWS s3 tools
- Rasterio from S3 investigations
- Utilities for data visualizations in notebooks

Installation
============

This repository provides a number of small [libraries](https://github.com/opendatacube/odc-tools/tree/develop/libs)
and [CLI tools](https://github.com/opendatacube/odc-tools/tree/develop/apps).

Full list of libraries, and install instructions:

- `odc.algo` algorithms (GeoMedian wrapper is here)
- `odc.stats` large scale processing framework (under development)
- `odc.ui` tools for data visualization in notebook/lab
- `odc.stac` STAC to ODC conversion tools (Moved to [odc-stac](https://github.com/opendatacube/odc-stac))
- `odc.dscache` experimental key-value store where `key=UUID`, `value=Dataset`
- `odc.io` common IO utilities, used by apps mainly
- `odc-cloud[ASYNC,AZURE,THREDDS]` cloud crawling support package
  - `odc.aws` AWS/S3 utilities, used by apps mainly
  - `odc.aio` faster concurrent fetching from S3 with async, used by apps `odc-cloud[ASYNC]`
  - `odc.{thredds,azure}` internal libs for cloud IO `odc-cloud[THREDDS,AZURE]`

Pre-release of these libraries is on PyPI now, so can be installed with `pip`
"the normal way". Most recent development versions of `odc-tools` packages are
pushed to `https://packages.dea.ga.gov.au`, and can be installed like so:

```
pip install --extra-index-url="https://packages.dea.ga.gov.au" \
  odc-ui \
  odc-stac \
  odc-stats \
  odc-algo \
  odc-io \
  odc-cloud[ASYNC] \
  odc-dscache
```

**NOTE**: on Ubuntu 18.04 the default `pip` version is awfully old and does not
support `--extra-index-url` command line option, so make sure to upgrade `pip`
first: `pip3 install --upgrade pip`.

For Conda Users
---------------

Currently there are no `odc-tools` conda packages. But majority of `odc-tools`
dependencies can be installed with conda from `conda-forge` channel.

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

  # odc.algo
  - dask-image
  - numexpr
  - scikit-image
  - scipy
  - toolz

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
  - mock
  - deepdiff

  # for pytest-depends
  - future_fstrings
  - networkx
  - colorama

  - pip=20
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
      - odc-algo
      - odc-ui
      - odc-dscache
      - odc-stats

      # odc-tools CLI apps
      - odc-apps-cloud
      - odc-apps-dc-tools
```
</div></details>

CLI Tools
=========

Installation
------------

Cloud tools depend on `aiobotocore` package which has a dependency on a specific
version of `botocore`. Another package we use, `boto3`, also depends on a
specific version of `botocore`. As a result having both `aiobotocore` and
`boto3` in one environment can be a bit tricky. The easiest way to solve this,
is to install `aiobotocore[awscli,boto3]` before anything else, which will pull
in a compatible version of `boto3` and `awscli` into the environment.

```
pip install -U "aiobotocore[awscli,boto3]==1.3.3"
# OR for conda setups
conda install "aiobotocore==1.3.3" boto3 awscli
```

The specific version of `aiobotocore` is not relevant, but it is needed in
practice to limit `pip`/`conda` package resolution search.


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

Requires docker, procedure was only tested on Linux hosts.

```bash
docker pull opendatacube/odc-test-runner:latest

cd odc-tools
make -C docker run-test
```

Above will run tests and generate test coverage report in `htmlcov/index.html`.

Other option is to run `make -C docker bash`, this will drop you into a shell in
`/code` folder that contains your current checkout of `odc-tools`. You can then
use `with-test-db start` command to launch and setup test database for running
integration tests that require datacube database to work. From here on you can
run specific tests you are developing with `py.test ./path/to/test_file.py`. Any
changes you make to code outside of the docker environment are available without
any further action from you for testing.


Release Process
===============

Development versions of packages are pushed to [DEA packages
repo](https://packages.dea.ga.gov.au/) on every push to `develop` branch,
version is automatically increased by a script that runs before creating wheels
and source distribution tar balls. Right now new dev version is pushed for all
the packages even the ones that have not changed since last push.

Publishing to [PyPi](https://pypi.org/) happens automatically when changes are
pushed to a protected `pypi/publish` branch. Only members of [Open Datacube
Admins](https://github.com/orgs/opendatacube/teams/admins) group have the
permission to push to this branch.

Process:

1. Manually edit `{lib,app}/{pkg}/odc/{pkg}/_version.py` file to increase version number
2. Merge it to `develop` branch via PR
3. Fast forward `pypi/publish` branch to match `develop`
4. Push it to GitHub

Steps 3 and 4 can be done by an authorized user with
`./scripts/sync-publish-branch.sh` script.
