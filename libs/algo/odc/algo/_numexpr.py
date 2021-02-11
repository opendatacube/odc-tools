from typing import Dict, Optional, Any
import dask
import dask.array as da
import numpy as np
import xarray as xr
import numexpr as ne
import functools
from ._dask import randomize, flatten_kv, unflatten_kv


def apply_numexpr_np(
    expr: str,
    data: Optional[Dict[str, Any]] = None,
    dtype=None,
    out: Optional[np.ndarray] = None,
    casting="safe",
    order="K",
    **params,
) -> np.ndarray:
    """
    Apply numexpr to numpy arrays
    """

    if data is None:
        data = params
    else:
        data.update(params)

    if out is None and dtype is not None:
        # This needs to be np.ndarray
        arrays = [x for x in data.values() if isinstance(x, np.ndarray)]
        if len(arrays) == 0:
            raise ValueError("Could not find any arrays on input")

        sample_input = arrays[0]
        out = np.empty_like(sample_input, dtype=dtype)

    return ne.evaluate(expr, local_dict=data, out=out, casting=casting, order=order)


def apply_numexpr(
    expr: str,
    xx: xr.Dataset,
    dtype=None,
    name="result",
    casting="safe",
    order="K",
    **params,
):
    """
    Apply numexpr to variables within a Dataset.

    numexpr library offers a limited subset of types and operations supported
    by numpy, but is much faster and memory efficient, particularly for complex
    expressions. See numexpr documentation for a more detailed explanation of
    performance advantages of using this library over numpy operations,
    summary: single pass over input memory, no temporary arrays, cache
    locality.

    :param expr: Numexpr compatible string to evaluate
    :param xx: Dataset object that contains arrays to be used in the ``expr`` (can be Dask)
    :param dtype: specify output dtype
    :param name: Used to name computation when input is Dask
    :param casting: Passed to ``numexpr.evaluate``
    :param order: Passed to ``numexpr.evaluate``
    :param params: Any other constants you use in the expression
    :raturns: xr.DataArray containing result of the equation (Dask is input is Dask)

    Example:

    .. code-block:: python

       # Given a Dataset with bands `red` and `nir`
       xx = dc.load(..., measurements=["red", "nir"], dask_chunks={})

       # Compute NDVI (ignore nodata for simplicity of the example)
       ndvi = apply_numexpr("(_1f*nir - red)/(_1f*nir + red)",
                            xx,
                            dtype='float32',   # Output is float32
                            _1f=np.float32(1)  # Define constant `_1f` being a float32(1),
                                               # used for casting to float32
                           )
    """

    bands = {}
    sample_band = None

    for band, x in xx.data_vars.items():
        band = str(band)

        if band in params:
            raise ValueError(f"Variable: `{band}` is aliased by a parameter")
        if band in expr:
            bands[band] = x.data

            if sample_band is None:
                sample_band = x

    if sample_band is None:
        raise ValueError("Found no bands on input")

    op = functools.partial(
        apply_numexpr_np, expr, dtype=dtype, casting=casting, order=order, **params
    )

    if dask.is_dask_collection(xx):
        # Passing through dictionary of Dask Arrays didn't work, so we have
        # adaptor that accepts var args in the form of [k0,v0,  k1,v1, ...] and then reconstructs dict
        data = da.map_blocks(
            lambda op, *bands : op(unflatten_kv(bands)),
            op,
            *flatten_kv(bands),
            name=randomize(name),
            dtype=dtype,
        )
    else:
        data = op(bands)

    return xr.DataArray(
        data=data,
        attrs=sample_band.attrs,
        dims=sample_band.dims,
        coords=sample_band.coords,
        name=name,
    )


def safe_div(x1: xr.DataArray, x2: xr.DataArray, dtype='float32') -> xr.DataArray:
    """
    Compute ``x1.astype(dtype)/x2.astype(dtype)`` taking care of cases where x2==0.

    For every element compute the following:

    ::

      x2 is 0 => NaN
      else    => float(x1)/float(x2)

    TODO: currently doesn't treat nodata values in any special way.
    """
    dtype = np.dtype(dtype)

    # TODO: support nodata on input
    return apply_numexpr("where(x2 == 0, nan, (_1f * x1) / x2)",
                         xr.Dataset(dict(x1=x1, x2=x2)),
                         dtype=dtype,
                         nan=dtype.type("nan"),
                         _1f=dtype.type(1))
