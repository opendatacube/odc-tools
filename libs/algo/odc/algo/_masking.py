""" Mostly masking related, also converting between float[with nans] and int[with nodata]

"""

import numpy as np
import xarray as xr
import dask
import dask.array as da
import numexpr as ne


def keep_good_np(xx, where, nodata):
    yy = np.full_like(xx, nodata)
    np.copyto(yy, xx, where=where)
    return yy


def keep_good_only(x, where,
                   inplace=False,
                   nodata=None):
    """ Return a copy of x, but with some pixels replaced with `nodata`.

    This function can work on dask arrays, in which case output will be a dask array as well.

    If x is a Dataset then operation will be applied to all data variables.

    :param x: xarray.DataArray with `nodata` property
    :param where: xarray.DataArray<bool> True -- keep, False -- replace with `x.nodata`
    :param inplace: Modify pixels in x directly, not valid for dask arrays.

    For every pixel of x[idx], output is:

     - nodata  if where[idx] == False
     - x[idx]  if where[idx] == True
    """
    if isinstance(x, xr.Dataset):
        return x.apply(lambda x: keep_good_only(x, where, inplace=inplace),
                       keep_attrs=True)

    assert x.shape == where.shape
    if nodata is None:
        nodata = getattr(x, 'nodata', 0)

    if inplace:
        if dask.is_dask_collection(x):
            raise ValueError("Can not perform inplace operation on a dask array")

        np.copyto(x.data, nodata, where=~where.data)
        return x

    if dask.is_dask_collection(x):
        data = da.map_blocks(keep_good_np,
                             x.data, where.data, nodata,
                             name='keep_good',
                             dtype=x.dtype)
    else:
        data = keep_good_np(x.data, where.data, nodata)

    return xr.DataArray(data,
                        dims=x.dims,
                        coords=x.coords,
                        attrs=x.attrs,
                        name=x.name)


def from_float_np(x, dtype, nodata, scale=1, offset=0, where=None):
    scale = np.float32(scale)
    offset = np.float32(offset)

    out = np.empty_like(x, dtype=dtype)

    params = dict(x=x,
                  nodata=nodata,
                  scale=scale,
                  offset=offset)

    # `x == x` is equivalent to `~np.isnan(x)`

    if where is not None:
        assert x.shape == where.shape
        params['m'] = where
        expr = 'where((x == x)&m, x*scale + offset, nodata)'
    else:
        expr = 'where(x == x, x*scale + offset, nodata)'

    ne.evaluate(expr,
                local_dict=params,
                out=out,
                casting='unsafe')

    return out


def to_f32_np(x, nodata=None, scale=1, offset=0):

    f32_nan = np.float32(np.nan)
    scale = np.float32(scale)
    offset = np.float32(offset)

    params = dict(f32_nan=f32_nan,
                  scale=scale,
                  offset=offset,
                  x=x,
                  nodata=nodata)

    if nodata is None:
        return ne.evaluate('x*scale + offset',
                           local_dict=params)
    elif scale == 1 and offset == 0:
        return ne.evaluate('where(x == nodata, f32_nan, x)',
                           local_dict=params)
    else:
        return ne.evaluate('where(x == nodata, f32_nan, x*scale + offset)',
                           local_dict=params)


def to_f32(x, scale=1, offset=0):
    if isinstance(x, xr.Dataset):
        return x.apply(to_f32,
                       scale=scale,
                       offset=offset,
                       keep_attrs=True)

    attrs = x.attrs.copy()
    nodata = attrs.pop('nodata', None)

    if dask.is_dask_collection(x.data):
        data = da.map_blocks(to_f32_np,
                             x.data,
                             nodata=nodata,
                             scale=scale,
                             offset=offset,
                             dtype='float32',
                             name='to_f32')
    else:
        data = to_f32_np(x.data,
                         nodata=nodata,
                         scale=scale,
                         offset=offset)

    return xr.DataArray(data,
                        dims=x.dims,
                        coords=x.coords,
                        name=x.name,
                        attrs=attrs)


def from_float(x, dtype, nodata, scale=1, offset=0):
    if isinstance(x, xr.Dataset):
        return x.apply(from_float, keep_attrs=True,
                       args=(dtype, nodata, scale, offset))

    attrs = x.attrs.copy()
    attrs['nodata'] = nodata

    if dask.is_dask_collection(x.data):
        data = da.map_blocks(from_float_np,
                             x.data, dtype, nodata,
                             scale=scale, offset=offset,
                             dtype=dtype,
                             name='from_float')
    else:
        data = from_float_np(x.data, dtype, nodata,
                             scale=scale, offset=offset)

    return xr.DataArray(data,
                        dims=x.dims,
                        coords=x.coords,
                        name=x.name,
                        attrs=attrs)
