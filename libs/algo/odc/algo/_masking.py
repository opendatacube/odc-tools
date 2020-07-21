""" Mostly masking related, also converting between float[with nans] and int[with nodata]

"""

from typing import Dict, Tuple, Any, Iterable, Union
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


def to_float_np(x, nodata=None, scale=1, offset=0, dtype='float32'):
    float_type = np.dtype(dtype).type

    _nan = float_type(np.nan)
    scale = float_type(scale)
    offset = float_type(offset)

    params = dict(_nan=_nan,
                  scale=scale,
                  offset=offset,
                  x=x,
                  nodata=nodata)
    out = np.empty_like(x, dtype=dtype)

    if nodata is None:
        return ne.evaluate('x*scale + offset',
                           out=out,
                           casting='unsafe',
                           local_dict=params)
    elif scale == 1 and offset == 0:
        return ne.evaluate('where(x == nodata, _nan, x)',
                           out=out,
                           casting='unsafe',
                           local_dict=params)
    else:
        return ne.evaluate('where(x == nodata, _nan, x*scale + offset)',
                           out=out,
                           casting='unsafe',
                           local_dict=params)


def to_f32_np(x, nodata=None, scale=1, offset=0):
    return to_float_np(x, nodata=nodata, scale=scale, offset=offset, dtype='float32')


def to_float(x, scale=1, offset=0, dtype='float32'):
    if isinstance(x, xr.Dataset):
        return x.apply(to_float,
                       scale=scale,
                       offset=offset,
                       dtype=dtype,
                       keep_attrs=True)

    attrs = x.attrs.copy()
    nodata = attrs.pop('nodata', None)

    if dask.is_dask_collection(x.data):
        data = da.map_blocks(to_float_np,
                             x.data, nodata, scale, offset, dtype,
                             dtype=dtype,
                             name=randomize('to_float'))
    else:
        data = to_float_np(x.data,
                           nodata=nodata,
                           scale=scale,
                           offset=offset,
                           dtype=dtype)

    return xr.DataArray(data,
                        dims=x.dims,
                        coords=x.coords,
                        name=x.name,
                        attrs=attrs)


def to_f32(x, scale=1, offset=0):
    return to_float(x, scale=scale, offset=offset, dtype='float32')


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


def _impl_to_bool(x, m):
    return ((1 << x) & m) > 0


def _impl_to_bool_inverted(x, m):
    return ((1 << x) & m) == 0


def _flags_invert(flags: Dict[str, Any]) -> Dict[str, Any]:
    _out = dict(**flags)
    _out['values'] = {n: int(v)
                      for v, n in flags['values'].items()}
    return _out


def _get_enum_values(names: Iterable[str],
                     flags_definition: Dict[str, Dict[str, Any]],
                     flag: str = '') -> Tuple[int, ...]:
    """
    Lookup enum values in flags definition library

    :param names: enum value to lookup e.g. ("cloud", "shadow")
    :param flags_definition: Flags definition dictionary as used by Datacube
    :param flag: Name of the enum (for example "fmask", auto-guessed if omitted)
    """
    if flag != '':
        flags_definition = {flag: flags_definition[flag]}

    names = list(names)
    names_set = set(names)
    unmatched = set()
    for ff in flags_definition.values():
        values = _flags_invert(ff)['values']
        unmatched = names_set - set(values.keys())
        if len(unmatched) == 0:
            return tuple(values[n] for n in names)

    if len(flags_definition) > 1:
        raise ValueError("Can not find flags definitions that match query")
    else:
        unmatched_human = ",".join(f'"{name}"' for name in unmatched)
        raise ValueError(f"Not all enumeration names were found: {unmatched_human}")


def _mk_ne_isin_condition(values: Tuple[int,...],
                          var_name: str = 'a',
                          invert: bool = False) -> str:
    """
    Produce numexpr expression equivalent to numpys `.isin`

     - ((a==v1)|(a==v2)|..|a==vn)   when invert=False
     - ((a!=v1)&(a!=v2)&..&a!=vn)   when invert=True
    """
    op1, op2 = ('!=', '&') if invert else ('==', '|')
    parts = [f'({var_name}{op1}{val})' for val in values]
    return f'({op2.join(parts)})'


def _enum_to_mask_numexpr(mask: np.ndarray,
                          classes: Tuple[int, ...],
                          invert: bool = False,
                          value_true: int = 1,
                          value_false: int = 0,
                          dtype: Union[str, np.dtype] = 'bool') -> np.ndarray:
    cond = _mk_ne_isin_condition(classes, 'm', invert=invert)
    expr = f"where({cond}, {value_true}, {value_false})"
    out = np.empty_like(mask, dtype=dtype)

    ne.evaluate(expr,
                local_dict=dict(m=mask),
                out=out,
                casting='unsafe')

    return out


def enum_to_bool(mask: xr.DataArray,
                 categories: Iterable[Union[str, int]],
                 invert: bool = False,
                 flag_name: str = '',
                 value_true: int = 1,
                 value_false: int = 0,
                 dtype: Union[str, np.dtype] = 'bool') -> xr.DataArray:
    """
    This method works for fmask and other "enumerated" masks

    It is equivalent to `np.isin(mask, categories)`

    example:
        xx = dc.load(.., measurements=['fmask', ...])
        no_cloud = enum_to_bool(xx.fmask, ('valid', 'snow', 'water'))

        xx.where(no_cloud).isel(time=0).nbar_red.plot.imshow()

    """
    categories_s = tuple(c for c in categories if isinstance(c, str))
    classes = tuple(c for c in categories if isinstance(c, int))

    if len(categories_s) > 0:
        flags = getattr(mask, 'flags_definition', None)
        if flags is None:
            raise ValueError('Missing flags_definition attribute')

        classes = classes + _get_enum_values(categories_s, flags, flag=flag_name)

    bmask = xr.apply_ufunc(_enum_to_mask_numexpr,
                           mask,
                           kwargs=dict(classes=classes,
                                       invert=invert,
                                       value_false=value_false,
                                       value_true=value_true,
                                       dtype=dtype),
                           keep_attrs=True,
                           dask='parallelized',
                           output_dtypes=[dtype])
    bmask.attrs.pop('flags_definition', None)
    bmask.attrs.pop('nodata', None)

    return bmask


def fmask_to_bool(mask: xr.DataArray,
                  categories: Iterable[str],
                  invert: bool = False,
                  flag_name: str = '', **kw) -> xr.DataArray:
    """
    This method works for fmask and other "enumerated" masks

    It is equivalent to `np.isin(mask, categories)`

    example:
        xx = dc.load(.., measurements=['fmask', ...])
        no_cloud = fmask_to_bool(xx.fmask, ('valid', 'snow', 'water'))

        xx.where(no_cloud).isel(time=0).nbar_red.plot.imshow()

    """
    return enum_to_bool(mask, categories, invert=invert, flag_name=flag_name, **kw)


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


