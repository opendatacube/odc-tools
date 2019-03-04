odc.dtools
==========

Tools for configuring rasterio dask workers for efficient COG access.

Installation
------------

```
pip install 'git+https://github.com/opendatacube/dea-proto.git#egg=odc_dtools&subdirectory=dtools'
```

Usage
-----

Given a connected `client = dask.distributed.Client(..)`, you can setup GDAL env
tuned to reading COGs from S3 like so (make sure to adjust `region_name` for
your setup):

```python
from odc.dtools import rio_activate

rio_activate(client, aws=dict(region_name='ap-southeast-2'))
```

To check current GDAL settings across all worker threads of a dask cluster do:

```python
from odc.dtools import rio_getenv

for cfg in rio_getenv(client):
    print(cfg)
```

Sensitive data like AWS keys will be redacted at the worker, to get those values
as is supply `sanitize=False`, but be careful when working with notebooks in
version controlled environment that keeps track of cell outputs.
