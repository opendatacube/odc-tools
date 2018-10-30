DEA Prototype Code
------------------

- AWS s3 tools
- Rasterio from S3 investigations

Installation
------------

To have fast fetching from S3 need to install with `[async]` option.

```
pip install 'git+https://github.com/opendatacube/dea-proto.git#egg=dea-proto[async]'
```

On Ubuntu to install globally

```
sudo -H pip3 install 'git+https://github.com/opendatacube/dea-proto.git#egg=dea-proto[async]'
```


CLI Tools
---------

1. `s3-find` list S3 bucket with wildcard
2. `s3-to-tar` fetch documents from S3 and dump them to tar archive 
3. `dc-index-from-tar` read yaml documents from tar archive and add them to datacube


Example:

```bash
#!/bin/bash

s3_src='s3://dea-public-data/L2/sentinel-2-nrt/'

s3-find "${s3_src}" '*yaml' | \
  s3-to-tar | \
    dc-index-from-tar --env s2 --ignore-lineage
```
