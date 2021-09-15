import click
import json

from ._cli_common import main

import xarray as xr

from odc.aio import S3Fetcher, s3_find_glob
from datacube.index.eo3 import prep_eo3
from odc.stac.transform import stac_transform
from odc.index import product_from_yaml
from odc.dscache import create_cache
from datacube.model import Dataset
from datacube import Datacube
from odc.algo import store_to_mem, to_rgba
from datacube.utils.cog import write_cog
from odc.algo import save_cog
from datacube.utils.dask import start_local_dask
from datacube.utils.rio import configure_s3_access


def bytes2ds_doc(data):
    if isinstance(data, bytes):
        data = data.decode("utf-8")
    stac_doc = json.loads(data)
    eo3_doc = stac_transform(stac_doc)
    ds_doc = prep_eo3(eo3_doc)
    return ds_doc


def blob2ds(blob, product):
    doc = bytes2ds_doc(blob.data)
    return Dataset(product, doc, uris=[blob.url])


def s3_fetch_dss(base, product, glob="*.json", s3=None):
    if s3 is None:
        s3 = S3Fetcher(aws_unsigned=True)
    
    glob = f"{base.strip('/')}/{glob}"
    blobs = s3(o.url for o in s3_find_glob(glob, skip_check=True, s3=s3))
    dss = (blob2ds(b, product) for b in blobs)
    return dss


def xr_to_mem(xx, client):
    data = store_to_mem(xx.data, client)
    return xr.DataArray(
        data=data, coords=xx.coords, dims=xx.dims, attrs=xx.attrs
    )


def save(xx, location, product_name, verbose, creds=None, rgb_bands=None):
    client = start_local_dask(
        nanny=False,
        n_workers=1,
        threads_per_worker=8,
        mem_safety_margin="0G",
        processes=False,
    )

    gdal_cfg = {"GDAL_CACHEMAX": 8 * (1 << 30)}
    configure_s3_access(aws_unsigned=True, cloud_defaults=True, **gdal_cfg)
    configure_s3_access(
        aws_unsigned=True, cloud_defaults=True, client=client, **gdal_cfg
    )

    rgba = to_rgba(xx.isel(time=0), clamp=(0, 3000), bands=rgba_bands)
    rgba = xr_to_mem(rgba, client)

    if verbose:
        print(f"Writing {location}/{product_name}.tif")

    save_cog(
        rgba,
        f"{location}/{product_name}.tif",
        blocksize=1024,
        compress="zstd",
        zstd_level=4,
        overview_levels=[],
        NUM_THREADS="ALL_CPUS",
        BIGTIFF="YES",
        SPARSE_OK=True,
        creds=creds,
    )


@main.command("generate-mosaic")
@click.argument("product", type=str)
@click.argument("input_prefix", type=str)
@click.argument("location", type=str)
@click.option("--bands", type=str)
@click.option("--verbose", "-v", is_flag=True, help="Be verbose")
def generate_mosaic(product, input_prefix, location, bands, verbose):
    """
    Generate mosaic overviews of the stats data.

    An intermediate cache file is generated and stored in the output location
    during this process.
    Note: The input bucket must be public otherwise the data can not be listed.
    """
    bands = bands.split(",")
    product = product_from_yaml(product)
    if verbose:
        print(f"Preparing mosaics for {product.name} product")

    dss = s3_fetch_dss(input_prefix, product, glob="*.json")

    dc = Datacube()
    xx = dc.load(
        datasets=dss,
        dask_chunks={"x": 2048, "y": 2048},
        resolution=(-120, 120),
        measurements=bands,
    )

    if bands is None:
        suffix = ''
    else:
        suffix = '_' + '_'.join(bands)

    if verbose:
        print(f"Writing {location}/{product.name}{suffix}.tif")
    xx = xx.squeeze('time').to_stacked_array('bands', ['x', 'y'])
    yy = save_cog(
        xx,
        f"{location}/{product.name}{suffix}.tif",
        blocksize=1024,
        compress="zstd",
        zstd_level=4,
        overview_levels=[],
        NUM_THREADS="ALL_CPUS",
        BIGTIFF="YES",
        SPARSE_OK=True,
    )
    
    yy.compute()


if __name__ == "__main__":
    cli()
