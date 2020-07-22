from typing import Optional, List, Iterable, Union, Dict
import xarray as xr

from odc.index import group_by_nothing, solar_offset
from odc.algo import enum_to_bool, xr_reproject
from datacube import Datacube
from datacube.model import Dataset
from datacube.utils.geometry import GeoBox
from datacube.testutils.io import native_geobox


def compute_native_load_geobox(dst_geobox: GeoBox,
                               ds: Dataset,
                               band: str,
                               buffer: Optional[float] = None):

    native = native_geobox(ds, basis=band)
    if buffer is None:
        buffer = 10*max(map(abs, native.resolution))

    return GeoBox.from_geopolygon(dst_geobox.extent.to_crs(native.crs).buffer(buffer),
                                  crs=native.crs,
                                  resolution=native.resolution,
                                  align=native.alignment)


def _split_by_grid(xx: xr.DataArray) -> List[xr.DataArray]:
    def extract(ii):
        yy = xx[ii]
        crs = xx.grid2crs[xx.grid.data[0]]
        yy.attrs.update(crs=crs)
        yy.attrs.pop('grid2crs', None)
        return yy

    return [extract(ii)
            for ii in xx.groupby(xx.grid).groups.values()]


def _pick_first(xx, axis=0, **kw):
    return xx[0]


def _fuse_bool(xx, axis=0, **kw):
    if xx.shape[0] == 1:
        return xx[0]

    return xx.max(axis=axis)


def _load_fused_mask_1(sources: xr.DataArray,
                       band: str,
                       geobox: GeoBox,
                       categories: Iterable[Union[str, int]],
                       invert: bool = False,
                       resampling: str = 'nearest',
                       groupby: Optional[str] = 'solar_day',
                       dask_chunks: Optional[Dict[str, int]] = None):
    ds, = sources.data[0]
    load_geobox = compute_native_load_geobox(geobox, ds, band)
    mm = ds.type.lookup_measurements([band])
    xx = Datacube.load_data(sources,
                            load_geobox,
                            mm,
                            dask_chunks=dask_chunks)[band]
    mask = enum_to_bool(xx, categories=categories, invert=invert, dtype='uint8', value_true=255)
    if groupby is not None:
        mask = mask.groupby(groupby).reduce(_fuse_bool)

    chunks = None
    if dask_chunks is not None:
        chunks = tuple(dask_chunks.get(ax, -1)
                       for ax in ('y', 'x'))

    return xr_reproject(mask, geobox, chunks=chunks, resampling=resampling) > 127


def load_masks(dss: List[Dataset],
               band: str,
               geobox: GeoBox,
               categories: Iterable[Union[str, int]],
               invert: bool = False,
               resampling: str = 'nearest',
               groupby: Optional[str] = 'solar_day',
               dask_chunks: Optional[Dict[str, int]] = None) -> xr.DataArray:
    if groupby is None:
        groupby = 'idx'

    sources = group_by_nothing(dss, solar_offset(geobox.extent))

    xx = [_load_fused_mask_1(srcs,
                             band,
                             geobox,
                             categories,
                             invert=invert,
                             resampling=resampling,
                             groupby=groupby,
                             dask_chunks=dask_chunks)
          for srcs in _split_by_grid(sources)]

    if len(xx) == 1:
        xx = xx[0]
    else:
        xx = xr.concat(xx, groupby)
        xx = xx.groupby(groupby).reduce(_fuse_bool)

    return xx
