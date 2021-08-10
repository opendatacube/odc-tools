""" Mostly masking related, also converting between float[with nans] and int[with nodata]

"""

from typing import Dict, Tuple, Any, Iterable, Union, Optional
from functools import partial
import numpy as np
import xarray as xr
import dask
import dask.array as da
from dask.highlevelgraph import HighLevelGraph
import numexpr as ne
from ._dask import randomize, _get_chunks_asarray


def default_nodata(dtype):
    """Default `nodata` for a given dtype
    - nan for float{*}
    - 0   for any other type
    """
    if dtype.kind == "f":
        return dtype.type(np.nan)
    return dtype.type(0)


def keep_good_np(xx, where, nodata, out=None):
    if out is None:
        out = np.full_like(xx, nodata)
    else:
        assert out.shape == xx.shape
        assert out.dtype == xx.dtype
        assert out is not xx
        out[:] = nodata
    np.copyto(out, xx, where=where)
    return out


def keep_good_only(x, where, inplace=False, nodata=None):
    """Return a copy of x, but with some pixels replaced with `nodata`.

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
        return x.apply(
            lambda x: keep_good_only(x, where, inplace=inplace, nodata=nodata), keep_attrs=True
        )

    assert x.shape == where.shape
    if nodata is None:
        nodata = getattr(x, "nodata", 0)

    if inplace:
        if dask.is_dask_collection(x):
            raise ValueError("Can not perform inplace operation on a dask array")

        np.copyto(x.data, nodata, where=~where.data)
        return x

    if dask.is_dask_collection(x):
        data = da.map_blocks(
            keep_good_np,
            x.data,
            where.data,
            nodata,
            name=randomize("keep_good"),
            dtype=x.dtype,
        )
    else:
        data = keep_good_np(x.data, where.data, nodata)

    return xr.DataArray(data, dims=x.dims, coords=x.coords, attrs=x.attrs, name=x.name)


def erase_bad_np(xx, where, nodata, out=None):
    if out is None:
        out = np.copy(xx)
    else:
        assert out.shape == xx.shape
        assert out.dtype == xx.dtype
        assert out is not xx
        out[:] = xx
    np.copyto(out, nodata, where=where)
    return out


def erase_bad(x, where, inplace=False, nodata=None):
    """
    Return a copy of x, but with some pixels replaced with `nodata`.

    This function can work on dask arrays, in which case output will be a dask array as well.

    If x is a Dataset then operation will be applied to all data variables.

    :param x: xarray.DataArray with `nodata` property
    :param where: xarray.DataArray<bool> True -- replace with `x.nodata` False -- keep as it were
    :param inplace: Modify pixels in x directly, not valid for dask arrays.

    For every pixel of x[idx], output is:

     - nodata  if where[idx] == True
     - x[idx]  if where[idx] == False
    """
    if isinstance(x, xr.Dataset):
        return x.apply(lambda x: erase_bad(x, where, inplace=inplace), keep_attrs=True)

    assert x.shape == where.shape
    if nodata is None:
        nodata = getattr(x, "nodata", 0)

    if inplace:
        if dask.is_dask_collection(x):
            raise ValueError("Can not perform inplace operation on a dask array")

        np.copyto(x.data, nodata, where=where.data)
        return x

    if dask.is_dask_collection(x):
        data = da.map_blocks(
            erase_bad_np,
            x.data,
            where.data,
            nodata,
            name=randomize("erase_bad"),
            dtype=x.dtype,
        )
    else:
        data = erase_bad_np(x.data, where.data, nodata)

    return xr.DataArray(data, dims=x.dims, coords=x.coords, attrs=x.attrs, name=x.name)


def from_float_np(x, dtype, nodata, scale=1, offset=0, where=None, out=None):
    scale = np.float32(scale)
    offset = np.float32(offset)

    if out is None:
        out = np.empty_like(x, dtype=dtype)
    else:
        assert out.shape == x.shape

    params = dict(x=x, nodata=nodata, scale=scale, offset=offset)

    # `x == x` is equivalent to `~np.isnan(x)`

    if where is not None:
        assert x.shape == where.shape
        params["m"] = where
        expr = "where((x == x)&m, x*scale + offset, nodata)"
    else:
        expr = "where(x == x, x*scale + offset, nodata)"

    ne.evaluate(expr, local_dict=params, out=out, casting="unsafe")

    return out


def to_float_np(x, nodata=None, scale=1, offset=0, dtype="float32", out=None):
    float_type = np.dtype(dtype).type

    _nan = float_type(np.nan)
    scale = float_type(scale)
    offset = float_type(offset)

    params = dict(_nan=_nan, scale=scale, offset=offset, x=x, nodata=nodata)
    if out is None:
        out = np.empty_like(x, dtype=dtype)
    else:
        assert out.shape == x.shape

    if nodata is None:
        return ne.evaluate(
            "x*scale + offset", out=out, casting="unsafe", local_dict=params
        )
    elif scale == 1 and offset == 0:
        return ne.evaluate(
            "where(x == nodata, _nan, x)", out=out, casting="unsafe", local_dict=params
        )
    else:
        return ne.evaluate(
            "where(x == nodata, _nan, x*scale + offset)",
            out=out,
            casting="unsafe",
            local_dict=params,
        )


def to_f32_np(x, nodata=None, scale=1, offset=0, out=None):
    return to_float_np(
        x, nodata=nodata, scale=scale, offset=offset, dtype="float32", out=out
    )


def to_float(x, scale=1, offset=0, dtype="float32"):
    if isinstance(x, xr.Dataset):
        return x.apply(
            to_float, scale=scale, offset=offset, dtype=dtype, keep_attrs=True
        )

    attrs = x.attrs.copy()
    nodata = attrs.pop("nodata", None)

    if dask.is_dask_collection(x.data):
        data = da.map_blocks(
            to_float_np,
            x.data,
            nodata,
            scale,
            offset,
            dtype,
            dtype=dtype,
            name=randomize("to_float"),
        )
    else:
        data = to_float_np(
            x.data, nodata=nodata, scale=scale, offset=offset, dtype=dtype
        )

    return xr.DataArray(data, dims=x.dims, coords=x.coords, name=x.name, attrs=attrs)


def to_f32(x, scale=1, offset=0):
    return to_float(x, scale=scale, offset=offset, dtype="float32")


def from_float(x, dtype, nodata, scale=1, offset=0):
    if isinstance(x, xr.Dataset):
        return x.apply(from_float, keep_attrs=True, args=(dtype, nodata, scale, offset))

    attrs = x.attrs.copy()
    attrs["nodata"] = nodata

    if dask.is_dask_collection(x.data):
        data = da.map_blocks(
            from_float_np,
            x.data,
            dtype,
            nodata,
            scale=scale,
            offset=offset,
            dtype=dtype,
            name=randomize("from_float"),
        )
    else:
        data = from_float_np(x.data, dtype, nodata, scale=scale, offset=offset)

    return xr.DataArray(data, dims=x.dims, coords=x.coords, name=x.name, attrs=attrs)


def _impl_to_bool(x, m):
    return ((1 << x) & m) > 0


def _impl_to_bool_inverted(x, m):
    return ((1 << x) & m) == 0


def _flags_invert(flags: Dict[str, Any]) -> Dict[str, Any]:
    _out = dict(**flags)
    _out["values"] = {n: int(v) for v, n in flags["values"].items()}
    return _out


def _get_enum_values(
    names: Iterable[str], flags_definition: Dict[str, Dict[str, Any]], flag: str = ""
) -> Tuple[int, ...]:
    """
    Lookup enum values in flags definition library

    :param names: enum value to lookup e.g. ("cloud", "shadow")
    :param flags_definition: Flags definition dictionary as used by Datacube
    :param flag: Name of the enum (for example "fmask", auto-guessed if omitted)
    """
    if flag != "":
        flags_definition = {flag: flags_definition[flag]}

    names = list(names)
    names_set = set(names)
    unmatched = set()
    for ff in flags_definition.values():
        values = _flags_invert(ff)["values"]
        unmatched = names_set - set(values.keys())
        if len(unmatched) == 0:
            return tuple(values[n] for n in names)

    if len(flags_definition) > 1:
        raise ValueError("Can not find flags definitions that match query")
    else:
        unmatched_human = ",".join(f'"{name}"' for name in unmatched)
        raise ValueError(f"Not all enumeration names were found: {unmatched_human}")


def _mk_ne_isin_condition(
    values: Tuple[int, ...], var_name: str = "a", invert: bool = False
) -> str:
    """
    Produce numexpr expression equivalent to numpys `.isin`

     - ((a==v1)|(a==v2)|..|a==vn)   when invert=False
     - ((a!=v1)&(a!=v2)&..&a!=vn)   when invert=True
    """
    op1, op2 = ("!=", "&") if invert else ("==", "|")
    parts = [f"({var_name}{op1}{val})" for val in values]
    return f"({op2.join(parts)})"


def _enum_to_mask_numexpr(
    mask: np.ndarray,
    classes: Tuple[int, ...],
    invert: bool = False,
    value_true: int = 1,
    value_false: int = 0,
    dtype: Union[str, np.dtype] = "bool",
) -> np.ndarray:
    cond = _mk_ne_isin_condition(classes, "m", invert=invert)
    expr = f"where({cond}, {value_true}, {value_false})"
    out = np.empty_like(mask, dtype=dtype)

    ne.evaluate(expr, local_dict=dict(m=mask), out=out, casting="unsafe")

    return out


def _disk(r: int, ndim: int = 2) -> np.ndarray:
    from skimage.morphology import disk

    kernel = disk(r)
    while kernel.ndim < ndim:
        kernel = kernel[np.newaxis]
    return kernel


def xr_apply_morph_op(
    xx: xr.DataArray, operation: str, radius: int = 1, **kw
) -> xr.DataArray:
    """
    Apply morphological operation to Dask based xarray Array

    :param kw: passed on to the underlying operation
      border_value

    """
    import dask_image.ndmorph

    ops = {
        "dilation": dask_image.ndmorph.binary_dilation,
        "erosion": dask_image.ndmorph.binary_erosion,
        "opening": dask_image.ndmorph.binary_opening,
        "closing": dask_image.ndmorph.binary_closing,
    }
    assert dask.is_dask_collection(xx.data)
    assert operation in ops

    kernel = _disk(radius, xx.ndim)
    data = ops[operation](xx.data, kernel, **kw)

    return xr.DataArray(data=data, coords=xx.coords, dims=xx.dims, attrs=xx.attrs)


def binary_erosion(xx: xr.DataArray, radius: int = 1, **kw) -> xr.DataArray:
    return xr_apply_morph_op(xx, "erosion", radius, **kw)


def binary_dilation(xx: xr.DataArray, radius: int = 1, **kw) -> xr.DataArray:
    return xr_apply_morph_op(xx, "dilation", radius, **kw)


def binary_opening(xx: xr.DataArray, radius: int = 1, **kw) -> xr.DataArray:
    return xr_apply_morph_op(xx, "opening", radius, **kw)


def binary_closing(xx: xr.DataArray, radius: int = 1, **kw) -> xr.DataArray:
    return xr_apply_morph_op(xx, "closing", radius, **kw)


def mask_cleanup_np(
    mask: np.ndarray,
    mask_filters: Iterable[Tuple[str, int]] = [("opening", 2), ("dilation", 5)],
) -> np.ndarray:
    """
    Apply morphological operations on given binary mask.

    :param mask: Binary image to process
    :param mask_filters: Iterable tuples of morphological operations to apply on mask
    """
    import skimage.morphology as morph

    assert mask.dtype == "bool"

    ops = dict(
        opening=morph.binary_opening,
        closing=morph.binary_closing,
        dilation=morph.binary_dilation,
        erosion=morph.binary_erosion,
    )

    for operation, radius in mask_filters:
        op = ops.get(operation, None)
        if op is None:
            raise ValueError(f"Not supported morphological operation: {operation}")
        if radius > 0:
            mask = op(mask, _disk(radius, mask.ndim))
    return mask


def _compute_overlap_depth(r: Iterable[int], ndim: int) -> Tuple[int, ...]:
    r = max(r)
    return (0,) * (ndim - 2) + (r, r)


def mask_cleanup(
    mask: xr.DataArray,
    mask_filters: Iterable[Tuple[str, int]] = [("opening", 2), ("dilation", 5)],
    name: Optional[str] = None
) -> xr.DataArray:
    """
    Apply morphological operations on given binary mask.
    As we fuse those operations into single Dask task, it could be faster to run.

    Default mask_filters value is bit-equivalent to ``mask |> opening |> dilation``.

    :param mask: Binary image to process
    :param mask_filters: iterable tuples of morphological operations - ("<operation>", <radius>) - to apply on mask, where
        operation: string, can be one of this morphological operations -
                closing  = remove small holes in cloud - morphological closing
                opening  = shrinks away small areas of the mask
                dilation = adds padding to the mask
                erosion  = shrinks bright regions and enlarges dark regions
        radius: int
    :param name: Used when building Dask graphs
    """

    data = mask.data
    if dask.is_dask_collection(data):
        if name is None:
            name = "mask_cleanup"
            r = []
            for _, radius in mask_filters:
                name = name + f"_{radius}"
                r.append(radius)

        depth = _compute_overlap_depth(r, data.ndim)
        data = data.map_overlap(
            partial(mask_cleanup_np, mask_filters=mask_filters), depth, boundary="none", name=randomize(name)
        )
    else:
        data = mask_cleanup_np(data, mask_filters=mask_filters)

    return xr.DataArray(data, attrs=mask.attrs, coords=mask.coords, dims=mask.dims)


def enum_to_bool(
    mask: xr.DataArray,
    categories: Iterable[Union[str, int]],
    invert: bool = False,
    flag_name: str = "",
    value_true: int = 1,
    value_false: int = 0,
    dtype: Union[str, np.dtype] = "bool",
    name: str = "enum_to_bool",
) -> xr.DataArray:
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
        flags = getattr(mask, "flags_definition", None)
        if flags is None:
            raise ValueError("Missing flags_definition attribute")

        classes = classes + _get_enum_values(categories_s, flags, flag=flag_name)

    op = partial(
        _enum_to_mask_numexpr,
        classes=classes,
        invert=invert,
        value_false=value_false,
        value_true=value_true,
        dtype=dtype,
    )

    if dask.is_dask_collection(mask.data):
        data = da.map_blocks(op, mask.data, name=randomize(name), dtype=dtype)
    else:
        data = op(mask)

    attrs = dict(mask.attrs)
    attrs.pop("flags_definition", None)
    attrs.pop("nodata", None)

    bmask = xr.DataArray(data=data, dims=mask.dims, coords=mask.coords, attrs=attrs)

    return bmask


def fmask_to_bool(
    mask: xr.DataArray,
    categories: Iterable[str],
    invert: bool = False,
    flag_name: str = "",
    **kw,
) -> xr.DataArray:
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
    params = dict(a=a, b=fallback, nodata=a.dtype.type(nodata))

    out = np.empty_like(a)

    if np.isnan(nodata):
        # a==a equivalent to `not isnan(a)`
        expr = "where(a==a, a, b)"
    else:
        expr = "where(a!=nodata, a, b)"

    return ne.evaluate(expr, local_dict=params, out=out, casting="unsafe")


def gap_fill(x: xr.DataArray, fallback: xr.DataArray, nodata=None, attrs=None):
    """Fill missing values in `x` with values from `fallback`.

    x,fallback are expected to be xarray.DataArray with identical shape and dtype.

        out[pix] = x[pix] if x[pix] != x.nodata else fallback[pix]
    """

    if nodata is None:
        nodata = getattr(x, "nodata", None)

    if nodata is None:
        nodata = default_nodata(x.dtype)
    else:
        nodata = x.dtype.type(nodata)

    if attrs is None:
        attrs = x.attrs.copy()

    if dask.is_dask_collection(x):
        data = da.map_blocks(
            _gap_fill_np,
            x.data,
            fallback.data,
            nodata,
            name=randomize("gap_fill"),
            dtype=x.dtype,
        )
    else:
        data = _gap_fill_np(x.data, fallback.data, nodata)

    return xr.DataArray(data, attrs=attrs, dims=x.dims, coords=x.coords, name=x.name)


def _first_valid_np(
    *aa: np.ndarray, nodata: Union[float, int, None] = None
) -> np.ndarray:
    out = aa[0].copy()
    if nodata is None:
        nodata = default_nodata(out.dtype)

    for a in aa[1:]:
        out = _gap_fill_np(out, a, nodata)

    return out


def _fuse_min_np(*aa: np.ndarray) -> np.ndarray:
    """
    Element wise min (propagates NaN values)
    """
    out = aa[0].copy()
    for a in aa[1:]:
        out = np.minimum(out, a, out=out)
    return out


def _fuse_max_np(*aa: np.ndarray) -> np.ndarray:
    """
    Element wise max (propagates NaN values)
    """
    out = aa[0].copy()
    for a in aa[1:]:
        out = np.maximum(out, a, out=out)
    return out


def _fuse_and_np(*aa: np.ndarray) -> np.ndarray:
    """
    Element wise bit and
    """
    assert len(aa) > 0
    out = aa[0].copy()
    for a in aa[1:]:
        out &= a
    return out


def _fuse_or_np(*aa: np.ndarray) -> np.ndarray:
    """
    Element wise bit or
    """
    assert len(aa) > 0
    out = aa[0].copy()
    for a in aa[1:]:
        out |= a
    return out


def _da_fuse_with_custom_op(xx: da.Array, op, name="fuse") -> da.Array:
    """
    Out[0, y, x] = op(In[0:1, y, x], In[1:2, y, x], In[2:3, y, x]...)

    """
    can_do_flat = all([ch == 1 for ch in xx.chunks[0]])
    if not can_do_flat:
        slices = [xx[i : i + 1] for i in range(xx.shape[0])]
        return da.map_blocks(op, *slices, name=name)

    chunks, shapes = _get_chunks_asarray(xx)
    dsk = {}
    name = randomize(name)
    for idx in np.ndindex(chunks.shape[1:]):
        blocks = chunks[(slice(None), *idx)].ravel()
        dsk[(name, 0, *idx)] = (op, *blocks)

    dsk = HighLevelGraph.from_collections(name, dsk, dependencies=[xx])
    shape = (1, *xx.shape[1:])
    chunks = ((1,), *xx.chunks[1:])

    return da.Array(dsk, name, shape=shape, dtype=xx.dtype, chunks=chunks)


def _fuse_with_custom_op(x: xr.DataArray, op, name="fuse") -> xr.DataArray:
    """
    Out[0, y, x] = op(In[0:1, y, x], In[1:2, y, x], In[2:3, y, x]...)

    Expects data in _,y,x order. Works on Dask inputs too.
    """
    if dask.is_dask_collection(x.data):
        data = _da_fuse_with_custom_op(x.data, op, name=name)
    else:
        slices = [x.data[i : i + 1] for i in range(x.shape[0])]
        data = op(*slices)

    coords = {k: v for k, v in x.coords.items()}
    coords[x.dims[0]] = x.coords[x.dims[0]][0:1]

    return xr.DataArray(data, attrs=x.attrs, dims=x.dims, coords=coords, name=x.name)


def _xr_fuse(xx, op, name):
    if isinstance(xx, xr.Dataset):
        return xx.map(partial(_xr_fuse, op=op, name=name))
    if xx.shape[0] <= 1:
        return xx

    return _fuse_with_custom_op(xx, op, name=name)


def _or_fuser(xx):
    """
    meant to be called by `xx.groupby(..).map(_or_fuser)`
    """
    return _xr_fuse(xx, _fuse_or_np, "fuse_or")


def _and_fuser(xx):
    """
    meant to be called by `xx.groupby(..).map(_and_fuser)`
    """
    return _xr_fuse(xx, _fuse_and_np, "fuse_and")


def _min_fuser(xx):
    """
    meant to be called by `xx.groupby(..).map(_min_fuser)`
    """
    return _xr_fuse(xx, _fuse_min_np, "fuse_min")


def _max_fuser(xx):
    """
    meant to be called by `xx.groupby(..).map(_max_fuser)`
    """
    return _xr_fuse(xx, _fuse_max_np, "fuse_max")


def choose_first_valid(x: xr.DataArray, nodata=None) -> xr.DataArray:
    """
    ``Out[0, y, x] = In[i, y, x]`` where ``i`` is index of the first slice
    with valid data in it for a pixel at ``y,x`` coordinate.

    Expects data in ``_, y, x`` order. Works on Dask inputs too.
    """
    if nodata is None:
        nodata = x.attrs.get("nodata", None)

    return _fuse_with_custom_op(
        x, partial(_first_valid_np, nodata=nodata), name="choose_first_valid"
    )


def _nodata_fuser(xx, **kw):
    """
    meant to be called by `xx.groupby(..).map(_nodata_fuser)`
    """
    if isinstance(xx, xr.Dataset):
        return xx.map(choose_first_valid, **kw)
    if xx.shape[0] <= 1:
        return xx
    return choose_first_valid(xx, **kw)


def _fuse_mean_np(*aa, nodata):
    assert len(aa) > 0

    out = aa[0].astype(np.float32)
    count = (aa[0] != nodata).astype(np.float32)
    for a in aa[1:]:
        out += a.astype(np.float32)
        count += (a != nodata)

    out -= (len(aa) - count) * nodata
    out = np.round(out / count).astype(aa[0].dtype)
    out[count == 0] = nodata
    return out
