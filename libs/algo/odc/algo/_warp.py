"""
Dask aware reproject implementation
"""
from typing import Tuple, Optional, Union, Dict, Any
import numpy as np
import xarray as xr
from affine import Affine
from dask import is_dask_collection
import dask.array as da
from dask.highlevelgraph import HighLevelGraph
from ._dask import randomize, crop_2d_dense, unpack_chunks, empty_maker
from datacube.utils.geometry import (
    GeoBox,
    rio_reproject,
    compute_reproject_roi,
    warp_affine,
)
from datacube.utils.geometry.gbox import GeoboxTiles
from datacube.utils import spatial_dims

from ._types import NodataType
from ._numeric import shape_shrink2


def _reproject_block_impl(
    src: np.ndarray,
    src_geobox: GeoBox,
    dst_geobox: GeoBox,
    resampling: str = "nearest",
    src_nodata: Optional[NodataType] = None,
    dst_nodata: Optional[NodataType] = None,
    axis: int = 0,
) -> np.ndarray:
    dst_shape = src.shape[:axis] + dst_geobox.shape + src.shape[axis + 2 :]
    dst = np.empty(dst_shape, dtype=src.dtype)

    if dst.ndim == 2 or (dst.ndim == 3 and axis == 1):
        rio_reproject(
            src, dst, src_geobox, dst_geobox, resampling, src_nodata, dst_nodata
        )
    else:
        for prefix in np.ndindex(src.shape[:axis]):
            rio_reproject(
                src[prefix],
                dst[prefix],
                src_geobox,
                dst_geobox,
                resampling,
                src_nodata,
                dst_nodata,
            )
    return dst


def _reproject_block_bool_impl(
    src: np.ndarray,
    src_geobox: GeoBox,
    dst_geobox: GeoBox,
    resampling: str = "nearest",
    src_nodata: Optional[NodataType] = None,
    dst_nodata: Optional[NodataType] = None,
    axis: int = 0,
) -> np.ndarray:
    assert src.dtype == "bool"
    assert src_nodata is None
    assert dst_nodata is None
    src = src.astype("uint8") << 7  # False:0, True:128
    dst = _reproject_block_impl(
        src, src_geobox, dst_geobox, resampling=resampling, axis=axis
    )
    return dst > 64


def dask_reproject(
    src: da.Array,
    src_geobox: GeoBox,
    dst_geobox: GeoBox,
    resampling: str = "nearest",
    chunks: Optional[Tuple[int, int]] = None,
    src_nodata: Optional[NodataType] = None,
    dst_nodata: Optional[NodataType] = None,
    axis: int = 0,
    name: str = "reproject",
) -> da.Array:
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
        chunks = src.chunksize[axis : axis + 2]

    if dst_nodata is None:
        dst_nodata = src_nodata

    assert src.shape[axis : axis + 2] == src_geobox.shape
    yx_shape = dst_geobox.shape
    yx_chunks = unpack_chunks(chunks, yx_shape)

    dst_chunks = src.chunks[:axis] + yx_chunks + src.chunks[axis + 2 :]
    dst_shape = src.shape[:axis] + yx_shape + src.shape[axis + 2 :]

    #  tuple(*dims1, y, x, *dims2) -- complete shape in blocks
    dims1 = tuple(map(len, dst_chunks[:axis]))
    dims2 = tuple(map(len, dst_chunks[axis + 2 :]))
    assert dims2 == ()
    deps = [src]

    tile_shape = (yx_chunks[0][0], yx_chunks[1][0])
    gbt = GeoboxTiles(dst_geobox, tile_shape)
    xy_chunks_with_data = list(gbt.tiles(src_geobox.extent))

    name = randomize(name)
    dsk: Dict[Any, Any] = {}

    block_impl = (
        _reproject_block_bool_impl if src.dtype == "bool" else _reproject_block_impl
    )

    for idx in xy_chunks_with_data:
        _dst_geobox = gbt[idx]
        rr = compute_reproject_roi(src_geobox, _dst_geobox)
        _src = crop_2d_dense(src, rr.roi_src, axis=axis)
        _src_geobox = src_geobox[rr.roi_src]

        deps.append(_src)

        for ii1 in np.ndindex(dims1):
            # TODO: band dims
            dsk[(name, *ii1, *idx)] = (
                block_impl,
                (_src.name, *ii1, 0, 0),
                _src_geobox,
                _dst_geobox,
                resampling,
                src_nodata,
                dst_nodata,
                axis,
            )

    fill_value = 0 if dst_nodata is None else dst_nodata
    shape_in_blocks = tuple(map(len, dst_chunks))

    mk_empty = empty_maker(fill_value, src.dtype, dsk)

    for idx in np.ndindex(shape_in_blocks):
        # TODO: other dims
        k = (name, *idx)
        if k not in dsk:
            bshape = tuple(ch[i] for ch, i in zip(dst_chunks, idx))
            dsk[k] = mk_empty(bshape)

    dsk = HighLevelGraph.from_collections(name, dsk, dependencies=deps)

    return da.Array(dsk, name, chunks=dst_chunks, dtype=src.dtype, shape=dst_shape)


def xr_reproject_array(
    src: xr.DataArray,
    geobox: GeoBox,
    resampling: str = "nearest",
    chunks: Optional[Tuple[int, int]] = None,
    dst_nodata: Optional[NodataType] = None,
) -> xr.DataArray:
    """
    Reproject DataArray to a given GeoBox

    :param src       : Input src[(time,) y,x (, band)]
    :param geobox    : GeoBox of the destination
    :param resampling: Resampling strategy as a string: nearest, bilinear, average, mode ...
    :param chunks    : In Y,X dimensions only, default is to use input chunk size
    :param dst_nodata: nodata marker for dst image (default is to use src.nodata)
    """
    src_nodata = getattr(src, "nodata", None)
    if dst_nodata is None:
        dst_nodata = src_nodata

    src_geobox = src.geobox
    assert src_geobox is not None

    yx_dims = spatial_dims(src)
    axis = tuple(src.dims).index(yx_dims[0])

    src_dims = tuple(src.dims)
    dst_dims = src_dims[:axis] + geobox.dims + src_dims[axis + 2 :]

    coords = geobox.xr_coords(with_crs=True)

    # copy non-spatial coords from src to dst
    src_non_spatial_dims = src_dims[:axis] + src_dims[axis + 2 :]
    for dim in src_non_spatial_dims:
        if dim not in coords:
            coords[dim] = src.coords[dim]

    attrs = {}
    if dst_nodata is not None:
        attrs["nodata"] = dst_nodata

    if is_dask_collection(src):
        data = dask_reproject(
            src.data,
            src_geobox,
            geobox,
            resampling=resampling,
            chunks=chunks,
            src_nodata=src_nodata,
            dst_nodata=dst_nodata,
            axis=axis,
        )
    else:
        data = _reproject_block_impl(
            src.data,
            src_geobox,
            geobox,
            resampling=resampling,
            src_nodata=src_nodata,
            dst_nodata=dst_nodata,
            axis=axis,
        )

    return xr.DataArray(data, name=src.name, coords=coords, dims=dst_dims, attrs=attrs)


def xr_reproject(
    src: Union[xr.DataArray, xr.Dataset],
    geobox: GeoBox,
    resampling: str = "nearest",
    chunks: Optional[Tuple[int, int]] = None,
    dst_nodata: Optional[NodataType] = None,
) -> Union[xr.DataArray, xr.Dataset]:
    """
    Reproject DataArray to a given GeoBox

    :param src       : Input src[(time,) y, x]
    :param geobox    : GeoBox of the destination
    :param resampling: Resampling strategy as a string: nearest, bilinear, average, mode ...
    :param chunks    : In Y,X dimensions only, default is to use input chunk size (ignored if input is not a dask array)
    :param dst_nodata: nodata marker for dst image (default is to use src.nodata)
    """

    if isinstance(src, xr.DataArray):
        return xr_reproject_array(
            src, geobox, resampling=resampling, chunks=chunks, dst_nodata=dst_nodata
        )

    bands = {
        name: xr_reproject_array(band, geobox, resampling=resampling, chunks=chunks)
        for name, band in src.data_vars.items()
    }

    return xr.Dataset(data_vars=bands)


def _shrink2(
    xx: np.ndarray,
    resampling: str = "nearest",
    nodata: Optional[NodataType] = None,
    axis: int = 0,
):
    """
    :param xx: Image to shrink
    :param resampling: Resampling strategy to use
    :param nodata: nodata value for missing value fill
    :param axis: Y-axis index, to distinguish Y,X,B (default) vs B,Y,X (axis=1)
    """
    out_shape = shape_shrink2(xx.shape, axis=axis)

    if xx.ndim == 2 or (xx.ndim == 3 and axis == 1):
        # [Y, X] or [B, Y, X]
        out = np.empty(out_shape, dtype=xx.dtype)
        warp_affine(
            xx,
            out,
            Affine.scale(2),
            resampling=resampling,
            src_nodata=nodata,
            dst_nodata=nodata,
        )
    elif xx.ndim == 3 and axis == 0:
        # [Y, X, B] -> [Y', X', B]
        assert xx.ndim == 3

        # Need to turn into B,Y,X order
        xx = xx.transpose((2, 0, 1))
        out = np.empty(out_shape[2:] + out_shape[:2], dtype=xx.dtype)
        warp_affine(
            xx,
            out,
            Affine.scale(2),
            resampling=resampling,
            src_nodata=nodata,
            dst_nodata=nodata,
        )

        # back to Y',X',B
        out = out.transpose((1, 2, 0))

        assert out_shape == out.shape
    else:
        raise ValueError("Only support Y,X | Y,X,B | B,Y,X inputs")

    return out
