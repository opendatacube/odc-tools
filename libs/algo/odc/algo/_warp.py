"""
Dask aware reproject implementation
"""
from typing import Tuple, Optional, Union, Dict, Any
import numpy as np
# import xarray as xr
import dask.array as da
from dask.highlevelgraph import HighLevelGraph
from ._dask import randomize, crop_2d_dense, unpack_chunksize, empty_maker
from datacube.utils.geometry import GeoBox, rio_reproject, compute_reproject_roi
from datacube.utils.geometry.gbox import GeoboxTiles

NodataType = Union[int, float]


def _reproject_block_impl(src: np.ndarray,
                          src_geobox: GeoBox,
                          dst_geobox: GeoBox,
                          resampling: str = 'nearest',
                          src_nodata: Optional[NodataType] = None,
                          dst_nodata: Optional[NodataType] = None,
                          axis: int = 0) -> np.ndarray:
    dst = np.empty(dst_geobox.shape, dtype=src.dtype)

    rio_reproject(src,
                  dst,
                  src_geobox,
                  dst_geobox,
                  resampling,
                  src_nodata,
                  dst_nodata)
    return dst


def dask_reproject(src: da.Array,
                   src_geobox: GeoBox,
                   dst_geobox: GeoBox,
                   resampling: str = "nearest",
                   chunks: Optional[Tuple[int, int]] = None,
                   src_nodata: Optional[NodataType] = None,
                   dst_nodata: Optional[NodataType] = None,
                   axis: int = 0,
                   name: str = "reproject") -> da.Array:
    """
    Reproject to GeoBox as dask operation

    :param src       : Input src[(time,) y,x (, band)]
    :param src_geobox: GeoBox of the source array
    :param dst_geobox: GeoBox of the destination
    :param resampling: Resampling strategy as a string: nearest, bilinear, average, mode ...
    :param chunks    : In Y,X dimensions only, default is to use same input chunk size
    :param axis      : Index of Y axis (default is 0)
    :param src_nodata: nodata marker for source image
    :param dst_nodata: nodata marker for dst image
    :param name      : Dask graph name, "reproject" is the default
    """
    if chunks is None:
        chunks = src.chunksize[axis:axis+2]

    if dst_nodata is None:
        dst_nodata = src_nodata

    assert src.shape[axis:axis+2] == src_geobox.shape
    yx_shape = dst_geobox.shape
    yx_chunks = tuple(unpack_chunksize(ch, n)
                      for ch, n in zip(chunks, yx_shape))

    dst_chunks = src.chunks[:axis] + yx_chunks + src.chunks[axis+2:]
    dst_shape = src.shape[:axis] + yx_shape + src.shape[axis+2:]

    #  tuple(*dims1, y, x, *dims2) -- complete shape in blocks
    # dims1 = tuple(map(len, dst_chunks[:axis]))
    # dims2 = tuple(map(len, dst_chunks[axis+2:]))
    deps = [src]

    gbt = GeoboxTiles(dst_geobox, chunks)
    xy_chunks_with_data = list(gbt.tiles(src_geobox.extent))

    name = randomize(name)
    dsk: Dict[Any, Any] = {}

    for idx in xy_chunks_with_data:
        _dst_geobox = gbt[idx]
        rr = compute_reproject_roi(src_geobox, _dst_geobox)
        _src = crop_2d_dense(src, rr.roi_src, axis=axis)
        _src_geobox = src_geobox[rr.roi_src]

        deps.append(_src)

        # TODO: other dims
        dsk[(name, *idx)] = (_reproject_block_impl,
                             (_src.name, 0, 0),
                             _src_geobox,
                             _dst_geobox,
                             resampling,
                             src_nodata,
                             dst_nodata,
                             axis)

    fill_value = 0 if dst_nodata is None else dst_nodata
    yx_shape_in_blocks = tuple(map(len, yx_chunks))

    mk_empty = empty_maker(fill_value, src.dtype, dsk)

    for idx in np.ndindex(yx_shape_in_blocks):
        # TODO: other dims
        k = (name, *idx)
        if k not in dsk:
            bshape = gbt.chunk_shape(idx)
            dsk[k] = mk_empty(bshape)

    dsk = HighLevelGraph.from_collections(name, dsk, dependencies=deps)

    return da.Array(dsk,
                    name,
                    chunks=dst_chunks,
                    dtype=src.dtype,
                    shape=dst_shape)
