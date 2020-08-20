from typing import Optional, List, Iterable, Union, Dict, Tuple, Callable
import xarray as xr
import numpy as np

from odc.index import group_by_nothing, solar_offset
from odc.algo import enum_to_bool, xr_reproject
from datacube import Datacube
from datacube.model import Dataset
from datacube.utils.geometry import GeoBox
from datacube.testutils.io import native_geobox
from ._masking import choose_first_valid, _choose_with_custom_op


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


def _nodata_fuser(xx, **kw):
    """
    meant to be called by `xx.groupby(..).map(_nodata_fuser)`
    """
    if isinstance(xx, xr.Dataset):
        return xx.map(choose_first_valid, **kw)
    if xx.shape[0] <= 1:
        return xx
    return choose_first_valid(xx, **kw)


def _load_with_native_transform_1(sources: xr.DataArray,
                                  bands: Tuple[str, ...],
                                  geobox: GeoBox,
                                  native_transform: Callable[[xr.Dataset], xr.Dataset],
                                  basis: Optional[str] = None,
                                  groupby: Optional[str] = None,
                                  fuser: Optional[Callable[[xr.Dataset], xr.Dataset]] = None,
                                  resampling: str = 'nearest',
                                  chunks: Optional[Dict[str, int]] = None) -> xr.Dataset:
    if basis is None:
        basis = bands[0]

    ds, = sources.data[0]
    load_geobox = compute_native_load_geobox(geobox, ds, basis)
    mm = ds.type.lookup_measurements(bands)
    xx = Datacube.load_data(sources,
                            load_geobox,
                            mm,
                            dask_chunks=chunks)
    xx = native_transform(xx)

    if groupby is not None:
        if fuser is None:
            fuser = _nodata_fuser
        xx = xx.groupby(groupby).map(fuser)

    _chunks = None
    if chunks is not None:
        _chunks = tuple(chunks.get(ax, -1)
                        for ax in ('y', 'x'))

    return xr_reproject(xx, geobox, chunks=_chunks, resampling=resampling)


def load_with_native_transform(dss: List[Dataset],
                               bands: Tuple[str, ...],
                               geobox: GeoBox,
                               native_transform: Callable[[xr.Dataset], xr.Dataset],
                               basis: Optional[str] = None,
                               groupby: Optional[str] = None,
                               fuser: Optional[Callable[[xr.Dataset], xr.Dataset]] = None,
                               resampling: str = 'nearest',
                               chunks: Optional[Dict[str, int]] = None,
                               **kw) -> xr.Dataset:
    """
    Load a bunch of datasets with native pixel transform.

    :param dss: A list of datasets to load
    :param bands: Which measurements to load
    :param geobox: GeoBox of the final output
    :param native_transform: ``xr.Dataset -> xr.Dataset`` transform, should support Dask inputs/outputs
    :param basis: Name of the band to use as a reference for what is "native projection"
    :param groupby: One of 'solar_day'|'time'|'idx'|None
    :param fuser: Optional ``xr.Dataset -> xr.Dataset`` transform
    :param resampling: Any resampling mode supported by GDAL as a string:
                       nearest, bilinear, average, mode, cubic, etc...
    :param chunks: If set use Dask, must be in dictionary form ``{'x': 4000, 'y': 4000}``
    :param kw: Used to support old names ``dask_chunks`` and ``group_by``

    1. Partition datasets by native Projection
    2. For every group do
       - Load data
       - Apply native_transform
       - [Optional] fuse rasters that happened on the same day/time
       - Reproject to final geobox
    3. Stack output of (2)
    4. [Optional] fuse rasters that happened on the same day/time

    """
    if fuser is None:
        fuser = _nodata_fuser

    if groupby is None:
        groupby = kw.get('group_by', 'idx')
    if chunks is None:
        chunks = kw.get('dask_chunks', None)

    sources = group_by_nothing(dss, solar_offset(geobox.extent))
    xx = [_load_with_native_transform_1(srcs,
                                        bands,
                                        geobox,
                                        native_transform,
                                        basis=basis,
                                        resampling=resampling,
                                        groupby=groupby,
                                        fuser=fuser,
                                        chunks=chunks)
          for srcs in _split_by_grid(sources)]

    if len(xx) == 1:
        xx = xx[0]
    else:
        xx = xr.concat(xx, sources.dims[0])
        if groupby != 'idx':
            xx = xx.groupby(groupby).map(fuser)

    # TODO: probably want to replace spec MultiIndex with just `time` component

    return xx


def _choose_min(*aa: np.ndarray) -> np.ndarray:
    """
    Element wise min (propagates NaN values)
    """
    out = aa[0].copy()
    for a in aa[1:]:
        out = np.minimum(out, a, out=out)
    return out


def _min_fuser(xx):
    """
    meant to be called by `xx.groupby(..).map(_min_fuser)`
    """
    if isinstance(xx, xr.Dataset):
        return xx.map(_min_fuser)
    if xx.shape[0] <= 1:
        return xx

    return _choose_with_custom_op(xx, _choose_min)


def _choose_max(*aa: np.ndarray) -> np.ndarray:
    """
    Element wise max (propagates NaN values)
    """
    out = aa[0].copy()
    for a in aa[1:]:
        out = np.maximum(out, a, out=out)
    return out


def _max_fuser(xx):
    """
    meant to be called by `xx.groupby(..).map(_max_fuser)`
    """
    if isinstance(xx, xr.Dataset):
        return xx.map(_max_fuser)
    if xx.shape[0] <= 1:
        return xx

    return _choose_with_custom_op(xx, _choose_max)


def load_enum_mask(dss: List[Dataset],
                   band: str,
                   geobox: GeoBox,
                   categories: Iterable[Union[str, int]],
                   invert: bool = False,
                   resampling: str = 'nearest',
                   groupby: Optional[str] = None,
                   chunks: Optional[Dict[str, int]] = None,
                   **kw) -> xr.DataArray:
    """
    Load enumerated mask (like fmask).

    1. Load each mask time slice separately in native projection of the file
    2. Convert enum to Boolean (F:0, T:255)
    3. Optionally (groupby='solar_day') group observations on the same day using OR for pixel fusing: T,F->T
    4. Reproject to destination GeoBox (any resampling mode is ok)
    5. Optionally group observations on the same day using OR for pixel fusing T,F->T
    6. Finally convert to real Bool
    """

    def native_op(ds):
        return ds.map(enum_to_bool,
                      categories=categories,
                      invert=invert,
                      dtype='uint8',
                      value_true=255)

    xx = load_with_native_transform(dss, [band],
                                    geobox,
                                    native_op,
                                    basis=band,
                                    resampling=resampling,
                                    groupby=groupby,
                                    chunks=chunks,
                                    fuser=_max_fuser,
                                    **kw)
    return xx[band] > 127
