""" Helper methods for Geometric Median computation.
"""
from typing import Optional, Tuple, Union
import numpy as np
import xarray as xr
import dask
import dask.array as da
import functools
from ._dask import randomize, reshape_yxbt
from ._masking import to_float_np, from_float_np
from ._memsink import yxbt_sink


def reshape_for_geomedian(ds, axis="time"):
    dims = set(v.dims for v in ds.data_vars.values())
    if len(dims) != 1:
        raise ValueError("All bands should have same dimensions")

    dims = dims.pop()
    if len(dims) != 3:
        raise ValueError("Expect 3 dimensions on input")

    if axis not in dims:
        raise ValueError(f"No such axis: {axis}")

    dims = tuple(d for d in dims if d != axis) + ("band", axis)

    nodata = set(getattr(v, "nodata", None) for v in ds.data_vars.values())
    if len(nodata) == 1:
        nodata = nodata.pop()
    else:
        nodata = None

    # xx: {y, x}, band, time
    xx = ds.to_array(dim="band").transpose(*dims)

    if nodata is not None:
        xx.attrs.update(nodata=nodata)

    return xx


def xr_geomedian(ds, axis="time", where=None, **kw):
    """

    :param ds: xr.Dataset|xr.DataArray|numpy array

    Other parameters:
    **kwargs -- passed on to pcm.gnmpcm
       maxiters   : int         1000
       eps        : float       0.0001
       num_threads: int| None   None
    """
    from hdstats import nangeomedian_pcm

    def norm_input(ds, axis):
        if isinstance(ds, xr.DataArray):
            xx = ds
            if len(xx.dims) != 4:
                raise ValueError("Expect 4 dimensions on input: y,x,band,time")
            if axis is not None and xx.dims[3] != axis:
                raise ValueError(
                    f"Can only reduce last dimension, expect: y,x,band,{axis}"
                )
            return None, xx, xx.data
        elif isinstance(ds, xr.Dataset):
            xx = reshape_for_geomedian(ds, axis)
            return ds, xx, xx.data
        else:  # assume numpy or similar
            xx_data = ds
            if xx_data.ndim != 4:
                raise ValueError("Expect 4 dimensions on input: y,x,band,time")
            return None, None, xx_data

    kw.setdefault("nocheck", True)
    kw.setdefault("num_threads", 1)
    kw.setdefault("eps", 1e-6)

    ds, xx, xx_data = norm_input(ds, axis)
    is_dask = dask.is_dask_collection(xx_data)

    if where is not None:
        if is_dask:
            raise NotImplementedError(
                "Dask version doesn't support output masking currently"
            )

        if where.shape != xx_data.shape[:2]:
            raise ValueError("Shape for `where` parameter doesn't match")
        set_nan = ~where
    else:
        set_nan = None

    if is_dask:
        if xx_data.shape[-2:] != xx_data.chunksize[-2:]:
            xx_data = xx_data.rechunk(xx_data.chunksize[:2] + (-1, -1))

        data = da.map_blocks(
            lambda x: nangeomedian_pcm(x, **kw),
            xx_data,
            name=randomize("geomedian"),
            dtype=xx_data.dtype,
            drop_axis=3,
        )
    else:
        data = nangeomedian_pcm(xx_data, **kw)

    if set_nan is not None:
        data[set_nan, :] = np.nan

    if xx is None:
        return data

    dims = xx.dims[:-1]
    cc = {k: xx.coords[k] for k in dims}
    xx_out = xr.DataArray(data, dims=dims, coords=cc)

    if ds is None:
        xx_out.attrs.update(xx.attrs)
        return xx_out

    ds_out = xx_out.to_dataset(dim="band")
    for b in ds.data_vars.keys():
        src, dst = ds[b], ds_out[b]
        dst.attrs.update(src.attrs)

    return ds_out


def _slices(step, n):
    if step < 0:
        yield slice(0, n)
        return

    for x in range(0, n, step):
        yield slice(x, min(x + step, n))


def int_geomedian_np(*bands, nodata=None, scale=1, offset=0, wk_rows=-1, **kw):
    """On input each band is expected to be same shape and dtype with 3 dimensions: time, y, x
    On output: band, y, x
    """
    from hdstats import nangeomedian_pcm

    nt, ny, nx = bands[0].shape
    dtype = bands[0].dtype
    nb = len(bands)
    gm_int = np.empty((nb, ny, nx), dtype=dtype)

    if wk_rows > ny or wk_rows <= 0:
        wk_rows = ny

    _wk_f32 = np.empty((wk_rows, nx, nb, nt), dtype="float32")

    for _y in _slices(wk_rows, ny):
        _ny = _y.stop - _y.start
        bb_f32 = _wk_f32[:_ny, ...]
        # extract part of the image with scaling
        for b_idx, b in enumerate(bands):
            for t_idx in range(nt):
                bb_f32[:, :, b_idx, t_idx] = to_float_np(
                    b[t_idx, _y, :],
                    nodata=nodata,
                    scale=scale,
                    offset=offset,
                    dtype="float32",
                )

        # run partial computation
        gm_f32 = nangeomedian_pcm(bb_f32, **kw)

        # extract results with scaling back
        for b_idx in range(nb):
            gm_int[b_idx, _y, :] = from_float_np(
                gm_f32[:, :, b_idx],
                dtype,
                nodata=nodata,
                scale=1 / scale,
                offset=-offset / scale,
            )

    return gm_int


def int_geomedian(ds, scale=1, offset=0, wk_rows=-1, as_array=False, **kw):
    """ds -- xr.Dataset (possibly dask) with dims: (time, y, x) for each band

        on output time dimension is removed

    :param ds: Dataset with int data variables
    :param scale: Normalize data for running computation (output is scaled back to original values)
    :param offset: ``(x*scale + offset)``
    :param wk_rows: reduce memory requirements by processing that many rows of a chunk at a time
    :param as_array: If set to True return DataArray with band dimension instead of Dataset
    :param kw: Passed on to hdstats (eps=1e-4, num_threads=1, maxiters=10_000, nocheck=True)

    """
    band_names = [dv.name for dv in ds.data_vars.values()]
    xx, *_ = ds.data_vars.values()
    nodata = getattr(xx, "nodata", None)

    is_dask = dask.is_dask_collection(xx)
    if is_dask:
        if xx.data.chunksize[0] != xx.shape[0]:
            ds = ds.chunk(chunks={xx.dims[0]: -1})
            xx, *_ = ds.data_vars.values()

    nt, ny, nx = xx.shape
    bands = [dv.data for dv in ds.data_vars.values()]
    band = bands[0]
    nb = len(bands)
    dtype = band.dtype

    kw.setdefault("nocheck", True)
    kw.setdefault("num_threads", 1)
    kw.setdefault("eps", 1e-4)
    kw.setdefault("maxiters", 10_000)

    if is_dask:
        chunks = ((nb,), *xx.chunks[1:])

        data = da.map_blocks(
            int_geomedian_np,
            *bands,
            nodata=nodata,
            scale=scale,
            offset=offset,
            wk_rows=wk_rows,
            **kw,
            name=randomize("geomedian"),
            dtype=dtype,
            chunks=chunks,
            drop_axis=[0],  # time is dropped
            new_axis=[0],
        )  # band is added on the left
    else:
        data = int_geomedian_np(
            *bands, nodata=nodata, scale=scale, offset=offset, wk_rows=wk_rows, **kw
        )

    dims = ("band", *xx.dims[1:])
    cc = {k: xx.coords[k] for k in dims[1:]}
    cc["band"] = band_names

    da_out = xr.DataArray(data, dims=dims, coords=cc)

    if as_array:
        if nodata is not None:
            da_out.attrs["nodata"] = nodata
        return da_out

    ds_out = da_out.to_dataset(dim="band")
    ds_out.attrs.update(ds.attrs)
    for b in ds.data_vars.keys():
        src, dst = ds[b], ds_out[b]
        dst.attrs.update(src.attrs)

    return ds_out


def _gm_mads_compute_f32(
    yxbt, compute_mads=True, compute_count=True, nodata=None, scale=1, offset=0, **kw
):
    """
    output axis order is:

      y, x, band

    When extra stats are compute they ar returned in the following order:
    [*bands, smad, emad, bcmad, count]

    note that when supplying non-float input, it is scaled according to scale/offset/nodata parameters,
    output is however returned in that scaled range.
    """
    import hdstats

    if yxbt.dtype.kind != "f":
        yxbt = to_float_np(yxbt, scale=scale, offset=offset, nodata=nodata)

    gm = hdstats.nangeomedian_pcm(yxbt, nocheck=True, **kw)

    stats_bands = []

    if compute_mads:
        mads = [hdstats.smad_pcm, hdstats.emad_pcm, hdstats.bcmad_pcm]

        for i, op in enumerate(mads):
            stats_bands.append(
                op(yxbt, gm, num_threads=kw.get("num_threads", 1))
            )

    if compute_count:
        nbads = np.isnan(yxbt).sum(axis=2, dtype="bool").sum(axis=2, dtype="uint16")
        count = yxbt.dtype.type(yxbt.shape[-1]) - nbads
        stats_bands.append(count)

    if len(stats_bands) == 0:
        return gm

    stats_bands = [a[..., np.newaxis] for a in stats_bands]

    return np.concatenate([gm, *stats_bands], axis=2)


def geomedian_with_mads(
    src: Union[xr.Dataset, xr.DataArray],
    compute_mads: bool = True,
    compute_count: bool = True,
    out_chunks: Optional[Tuple[int, int, int]] = None,
    reshape_strategy: str = "mem",
    scale: float = 1.0,
    offset: float = 0.0,
    eps: Optional[float] = None,
    maxiters: int = 1000,
    num_threads: int = 1,
    **kw,
) -> xr.Dataset:
    """
    Compute Geomedian on Dask backed Dataset.

    NOTE: Default configuration of this code assumes that entire input can be
    loaded in to RAM on the Dask worker. It also assumes that there is only one
    worker in the cluster, or that entire task will get scheduled on one single
    worker only. See ``reshape_strategy`` parameter.

    :param src: xr.Dataset or a single array in YXBT order, bands can be either
                float or integer with `nodata` values to indicate gaps in data.

    :param compute_mads: Whether to compute smad,emad,bcmad statistics

    :param compute_count: Whether to compute count statistic (number of
                          contributing observations per output pixels)

    :param out_chunks: Advanced option, allows to rechunk output internally,
                       order is ``(ny, nx, nband)``

    :param reshape_strategy: One of ``mem`` (default) or ``yxbt``. This is only
    applicable when supplying Dataset object. It controls how Dataset is
    reshaped into DataArray in the format expected by Geomedian code. If you
    have enough RAM and use single-worker Dask cluster, then use ``mem``, it
    should be the most efficient. If there is not enough RAM to load entire
    input you can try ``yxbt`` mode, but you might still run out of RAM anyway.
    If using multi-worker Dask cluster you have to use ``yxbt`` strategy.

    :param scale, offset: Only used when input contains integer values, actual
                          Geomedian will run on scaled values
                          ``scale*X+offset``. Only affects internal
                          computation, final result is scaled back to the
                          original value range.

    :param eps: Termination criteria passed on to geomedian algorithm

    :param maxiters: Maximum number of iterations done per output pixel

    :param num_threads: Configure internal concurrency of the Geomedian
                        computation. Default is 1 as we assume that Dask will
                        run a bunch of those concurrently.

    :param work_chunks: Default is ``(100, 100)``, only applicable when input
                        is Dataset.
    """
    if not dask.is_dask_collection(src):
        raise ValueError("This method only works on Dask inputs")

    if isinstance(src, xr.DataArray):
        yxbt = src
    else:
        # TODO: better automatic defaults for work_chunks
        ny, nx = kw.get("work_chunks", (100, 100))
        if reshape_strategy == "mem":
            yxbt = yxbt_sink(src, (ny, nx, -1, -1))
        elif reshape_strategy == "yxbt":
            yxbt = reshape_yxbt(src, yx_chunks=(ny, nx))
        else:
            raise ValueError(
                f"Reshape strategy '{reshape_strategy}' not understood use one of: mem or yxbt"
            )

    ny, nx, nb, nt = yxbt.shape
    nodata = yxbt.attrs.get("nodata", None)
    assert yxbt.chunks is not None
    if yxbt.data.numblocks[2:4] != (1, 1):
        raise ValueError("There should be one dask block along time and band dimension")

    n_extras = (3 if compute_mads else 0) + (1 if compute_count else 0)
    chunks = (*yxbt.chunks[:2], (nb + n_extras,))

    is_float = yxbt.dtype.kind == "f"

    if eps is None:
        eps = 1e-4 if is_float else 0.1 * scale

    op = functools.partial(
        _gm_mads_compute_f32,
        compute_mads=compute_mads,
        compute_count=compute_count,
        nodata=nodata,
        scale=scale,
        offset=offset,
        eps=eps,
        maxiters=maxiters,
        num_threads=num_threads,
    )

    _gm = da.map_blocks(
        op, yxbt.data, dtype="float32", drop_axis=3, chunks=chunks, name="geomedian"
    )
    if out_chunks is not None:
        _gm = _gm.rechunk(out_chunks)

    gm_data = _gm[:, :, :nb]
    if not is_float:
        gm_data = da.map_blocks(
            lambda x: from_float_np(
                x, yxbt.dtype, nodata, scale=1 / scale, offset=-offset / scale
            ),
            gm_data,
            dtype=yxbt.dtype,
        )

    dims = yxbt.dims[:3]
    coords = {k: yxbt.coords[k] for k in dims}
    result = xr.DataArray(
        data=gm_data, dims=dims, coords=coords, attrs=yxbt.attrs
    ).to_dataset("band")

    for dv in result.data_vars.values():
        dv.attrs.update(yxbt.attrs)

    next_stat = nb
    if compute_mads:
        smad = _gm[:, :, next_stat + 0]
        emad = _gm[:, :, next_stat + 1]
        bcmad = _gm[:, :, next_stat + 2]
        next_stat += 3

        if not is_float:
            emad = emad * (1 / scale)

        result["smad"] = xr.DataArray(data=smad, dims=dims[:2], coords=result.coords)
        result["emad"] = xr.DataArray(data=emad, dims=dims[:2], coords=result.coords)
        result["bcmad"] = xr.DataArray(data=bcmad, dims=dims[:2], coords=result.coords)

    if compute_count:
        count = _gm[:, :, next_stat].astype("uint16")
        next_stat += 1
        result["count"] = xr.DataArray(data=count, dims=dims[:2], coords=result.coords)

    return result
