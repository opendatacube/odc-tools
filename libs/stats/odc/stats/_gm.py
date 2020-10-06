"""
Sentinel-2 Geomedian
"""
from typing import Optional, Tuple, Union
import xarray as xr

from odc.stats.model import Task
from odc.algo.io import load_with_native_transform
from odc.algo import enum_to_bool, int_geomedian, keep_good_only
from .model import OutputProduct


bad_pixel_classes = (0, 'saturated or defective')
cloud_classes = ('cloud shadows',
                 'cloud medium probability',
                 'cloud high probability',
                 'thin cirrus')


def gm_product(location: Optional[str] = None,
               bands: Optional[Tuple[str, ...]] = None) -> OutputProduct:
    name = 'ga_s2_gm'
    short_name = 'ga_s2_gm'
    version = '0.0.0'

    if bands is None:
        bands = ('B02', 'B03', 'B04',
                 'B05', 'B06', 'B07', 'B08', 'B8A',
                 'B11', 'B12')

    if location is None:
        bucket = 'deafrica-stats-processing'
        location = f's3://{bucket}/{name}/v{version}'
    else:
        location = location.rstrip('/')

    measurements = tuple(bands)

    properties = {
        'odc:file_format': 'GeoTIFF',
        'odc:producer': 'ga.gov.au',
        'odc:product_family': 'statistics'  # TODO: ???
    }

    return OutputProduct(name=name,
                         version=version,
                         short_name=short_name,
                         location=location,
                         properties=properties,
                         measurements=measurements,
                         href=f'https://collections.digitalearth.africa/product/{name}')


def _gm_native_transform(xx: xr.Dataset) -> xr.Dataset:
    """
    config:
    bad_pixel_classes
    cloud_classes
    """

    # data bands are everything but SCL
    bands = list(xx.data_vars)
    bands.remove('SCL')

    # TODO: fancier computation of clear_pix with padding for cloud
    clear_pix = enum_to_bool(xx.SCL, bad_pixel_classes+cloud_classes, invert=True)

    xx = keep_good_only(xx[bands], clear_pix)

    return xx


def gm_input_data(task: Task, resampling: str, chunk: Union[int, Tuple[int, int]] = 1600) -> xr.Dataset:
    """
    .valid  Bool
    .clear  Bool
    """
    if isinstance(chunk, int):
        chunk = (chunk, chunk)

    xx = load_with_native_transform(task.datasets,
                                    ['SCL', *task.product.measurements],
                                    task.geobox,
                                    _gm_native_transform,
                                    groupby='solar_day',
                                    resampling=resampling,
                                    chunks={'y': chunk[0],
                                            'x': chunk[1]})
    return xx


def gm_reduce(xx: xr.Dataset,
              num_threads: int = 4,
              wk_rows: int = 64,
              as_array: bool = False) -> Union[xr.Dataset, xr.DataArray]:
    """
    """
    return int_geomedian(xx,
                         scale=1/10_000,
                         wk_rows=wk_rows,
                         as_array=as_array,
                         eps=1e-4,
                         num_threads=num_threads,
                         maxiters=1_000)
