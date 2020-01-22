""" Mostly masking related, also converting between float[with nans] and int[with nodata]

"""

import numpy as np
import xarray as xr
import dask
import dask.array as da
import numexpr as ne
from ._dask import randomize


def default_nodata(dtype):
    """ Default `nodata` for a given dtype
        - nan for float{*}
        - 0   for any other type
    """
    if dtype.kind == 'f':
        return dtype.type(np.nan)
    return dtype.type(0)


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
                             name=randomize('keep_good'),
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
                             name=randomize('to_f32'))
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
                             name=randomize('from_float'))
    else:
        data = from_float_np(x.data, dtype, nodata,
                             scale=scale, offset=offset)

    return xr.DataArray(data,
                        dims=x.dims,
                        coords=x.coords,
                        name=x.name,
                        attrs=attrs)


def fmask_to_bool(fmask, categories, invert=False):
    """

    example:
        xx = dc.load(.., measurements=['fmask', ...])
        no_cloud = fmask_to_bool(xx.fmask, ('valid', 'snow', 'water'))

        xx.where(no_cloud).isel(time=0).nbar_red.plot.imshow()
    """

    def _get_mask(names, flags):
        enum_to_value = {n: int(v)
                         for v, n in flags['values'].items()}
        m = 0
        for n in names:
            v = enum_to_value.get(n, None)
            if v is None:
                raise ValueError(f'`{n}` is not a valid class name')
            m |= (1 << v)
        return m

    flags = getattr(fmask, 'flags_definition', None)
    if flags is None:
        raise ValueError('Missing flags_definition attribute')

    if len(flags) == 1:
        flags, = flags.values()
    else:
        flags = flags.get('fmask', None)
        if flags is None:
            raise ValueError('Expect `fmask` key in `flags_defition` attribute')

    m = _get_mask(categories, flags)
    func = {False: lambda x: ((1 << x) & m) > 0,
            True: lambda x: ((1 << x) & m) == 0}.get(invert)

    mask = xr.apply_ufunc(func, fmask,
                          keep_attrs=True,
                          dask='parallelized',
                          output_dtypes=['bool'])
    mask.attrs.pop('flags_definition', None)
    mask.attrs.pop('nodata', None)

    return mask


def _gap_fill_np(a, fallback, nodata):
    params = dict(a=a,
                  b=fallback,
                  nodata=a.dtype.type(nodata))

    out = np.empty_like(a)

    if np.isnan(nodata):
        # a==a equivalent to `not isnan(a)`
        expr = 'where(a==a, a, b)'
    else:
        expr = 'where(a!=nodata, a, b)'

    return ne.evaluate(expr,
                       local_dict=params,
                       out=out,
                       casting='unsafe')


def gap_fill(x: xr.DataArray,
             fallback: xr.DataArray,
             nodata=None,
             attrs=None):
    """ Fill missing values in `x` with values from `fallback`.

        x,fallback are expected to be xarray.DataArray with identical shape and dtype.

            out[pix] = x[pix] if x[pix] != x.nodata else fallback[pix]
    """

    if nodata is None:
        nodata = getattr(x, 'nodata', None)

    if nodata is None:
        nodata = default_nodata(x.dtype)
    else:
        nodata = x.dtype.type(nodata)

    if attrs is None:
        attrs = x.attrs.copy()

    if dask.is_dask_collection(x):
        data = da.map_blocks(_gap_fill_np,
                             x.data, fallback.data, nodata,
                             name=randomize('gap_fill'),
                             dtype=x.dtype)
    else:
        data = _gap_fill_np(x.data, fallback.data, nodata)

    return xr.DataArray(data,
                        attrs=attrs,
                        dims=x.dims,
                        coords=x.coords,
                        name=x.name)


def test_gap_fill():
    a = np.zeros((5,), dtype='uint8')
    b = np.empty_like(a)
    b[:] = 33

    a[0] = 11
    ab = _gap_fill_np(a, b, 0)
    assert ab.dtype == a.dtype
    assert ab.tolist() == [11, 33, 33, 33, 33]

    xa = xr.DataArray(a,
                      name='test_a',
                      dims=('t',),
                      attrs={'p1': 1, 'nodata': 0},
                      coords=dict(t=np.arange(a.shape[0])))
    xb = xa + 0
    xb.data[:] = b
    xab = gap_fill(xa, xb)
    assert xab.name == xa.name
    assert xab.attrs == xa.attrs
    assert xab.data.tolist() == [11, 33, 33, 33, 33]

    xa.attrs['nodata'] = 11
    assert gap_fill(xa, xb).data.tolist() == [33, 0, 0, 0, 0]

    a = np.zeros((5,), dtype='float32')
    a[1:] = np.nan
    b = np.empty_like(a)
    b[:] = 33
    ab = _gap_fill_np(a, b, np.nan)

    assert ab.dtype == a.dtype
    assert ab.tolist() == [0, 33, 33, 33, 33]

    xa = xr.DataArray(a,
                      name='test_a',
                      dims=('t',),
                      attrs={'p1': 1},
                      coords=dict(t=np.arange(a.shape[0])))
    xb = xa + 0
    xb.data[:] = b
    xab = gap_fill(xa, xb)
    assert xab.name == xa.name
    assert xab.attrs == xa.attrs
    assert xab.data.tolist() == [0, 33, 33, 33, 33]

    xa = xr.DataArray(da.from_array(a),
                      name='test_a',
                      dims=('t',),
                      attrs={'p1': 1},
                      coords=dict(t=np.arange(a.shape[0])))

    xb = xr.DataArray(da.from_array(b),
                      name='test_a',
                      dims=('t',),
                      attrs={'p1': 1},
                      coords=dict(t=np.arange(b.shape[0])))

    assert dask.is_dask_collection(xa)
    assert dask.is_dask_collection(xb)
    xab = gap_fill(xa, xb)

    assert dask.is_dask_collection(xab)
    assert xab.name == xa.name
    assert xab.attrs == xa.attrs
    assert xab.compute().values.tolist() == [0, 33, 33, 33, 33]
