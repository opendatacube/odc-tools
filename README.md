DEA Prototype Code
------------------

- AWS s3 tools
- Rasterio from S3 investigations

Installation
------------

Using `pip`:

```
pip install 'git+https://github.com/opendatacube/dea-proto.git'
```

On Ubuntu to install globally

```
sudo -H pip3 install 'git+https://github.com/opendatacube/dea-proto.git'
```

NOTE: this lib depends on `aiobotocore` which has a dependency on a specific
version of `botocore`, `boto3` also depends on a specific version of `botocore`
as a result having both `aiobotocore` and `boto3` in one environment can be a bit
tricky. The easiest way to solve this is to install `aiobotocore[awscli,boto3]` before
anything else, which will pull in a compatible version of `boto3` and `awscli` into the
environment.

```
sudo pip3 install -U 'aiobotocore[awscli,boto3]'
```


CLI Tools
---------

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

```bash
#!/bin/bash

# Google Storage support
gs-to-tar --bucket data.deadev.com --prefix mangrove_cover
dc-index-from-tar --env mangroves --ignore-lineage metadata.tar.gz
```