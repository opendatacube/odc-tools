""" Helpers for dealing with RGB(A) images.
"""
import numpy as np
import dask.array as da
import xarray as xr
import dask
from typing import Tuple, Optional, List, Union
from ._dask import randomize


def is_rgb(x: xr.DataArray):
    if x.dtype != "uint8":
        return False
    if x.ndim < 3:
        return False
    if x.shape[-1] not in (3, 4):
        return False
    return True


def guess_rgb_names(bands: List[str]) -> Tuple[str, str, str]:
    out = []
    for c in ("red", "green", "blue"):
        candidates = [name for name in bands if c in name]
        n = len(candidates)
        if n == 0:
            raise ValueError('Found no candidate for color "{}"'.format(c))
        elif n > 1:
            raise ValueError('Found too many candidates for color "{}"'.format(c))

        out.append(candidates[0])
    r, g, b = out
    return (r, g, b)


def auto_guess_clamp(ds: xr.Dataset):
    # TODO: deal with nodata > 0 case
    return (0, max(x.data.max() for x in ds.data_vars.values()))


def to_u8(x: np.ndarray, x_min: float, x_max: float) -> np.ndarray:
    x = np.clip(x, x_min, x_max)

    if x.dtype.kind == "f":
        x = (x - x_min) * (255.0 / (x_max - x_min))
    else:
        x = (x - x_min).astype("uint32") * 255 // (x_max - x_min)
    return x.astype("uint8")


def to_rgba_np(
    r: np.ndarray,
    g: np.ndarray,
    b: np.ndarray,
    nodata: Optional[float],
    clamp: Tuple[float, float],
) -> np.ndarray:
    rgba = np.zeros((*r.shape, 4), dtype="uint8")

    if r.dtype.kind == "f":
        valid = ~np.isnan(r)
        if nodata is not None:
            valid = valid * (r != nodata)
    elif nodata is not None:
        valid = r != nodata
    else:
        valid = np.ones(r.shape, dtype=np.bool)

    rgba[..., 3] = valid.astype("uint8") * (0xFF)
    for idx, band in enumerate([r, g, b]):
        rgba[..., idx] = to_u8(band, *clamp)

    return rgba


def to_rgba(
    ds: xr.Dataset,
    clamp: Optional[Union[float, Tuple[float, float]]] = None,
    bands: Optional[Tuple[str, str, str]] = None,
) -> xr.DataArray:
    """Given `xr.Dataset` with bands `red,green,blue` construct `xr.Datarray`
        containing uint8 rgba image.

    :param ds: xarray Dataset
    :param clamp: [min_intensity, max_intensity] | max_intensity == [0, max_intensity]
                  Can also supply None for non-dask array, in which case clamp is set to [0, max(r,g,b)]
    :param bands: Which bands to use, order should be red,green,blue
    """
    if bands is None:
        bands = guess_rgb_names(list(ds.data_vars))

    is_dask = dask.is_dask_collection(ds)

    if clamp is None:
        if is_dask:
            raise ValueError("Must specify clamp for dask inputs")
        else:
            clamp = auto_guess_clamp(ds[list(bands)])
    elif not isinstance(clamp, tuple):
        clamp = (0, clamp)

    red_band = ds[bands[0]]
    nodata = getattr(red_band, "nodata", None)
    dims = red_band.dims + ("band",)
    geobox = red_band.geobox
    crs = str(geobox.crs) if geobox is not None else None

    r, g, b = (ds[name].data for name in bands)
    if is_dask:
        data = da.map_blocks(
            to_rgba_np,
            r,
            g,
            b,
            nodata,
            clamp,
            dtype=np.uint8,
            new_axis=[r.ndim],
            name=randomize("ro_rgba"),
            chunks=(*red_band.chunks, 4),
        )
    else:
        data = to_rgba_np(r, g, b, nodata, clamp)

    coords = {name: coord for name, coord in red_band.coords.items()}
    coords.update(band=["r", "g", "b", "a"])

    attrs = {}
    if crs is not None:
        attrs["crs"] = crs

    rgba = xr.DataArray(data, coords=coords, dims=dims, attrs=attrs)
    return rgba


def colorize(x: xr.DataArray, cmap: np.ndarray, attrs=None) -> xr.DataArray:
    """
    Map categorical values from x to RGBA according to cmap lookup table

    :param x: Input xarray data array (can be Dask)
    :param cmap: Lookup table cmap[x] -> RGB(A)
    :param attrs: xarray attributes table, if not supplied input attributes are copied across
    """
    assert cmap.ndim == 2
    assert cmap.shape[1] in (3, 4)

    if attrs is None:
        attrs = x.attrs

    dims = x.dims + ("band",)
    coords = dict(x.coords.items())
    coords["band"] = ["r", "g", "b", "a"]

    if dask.is_dask_collection(x.data):
        data = da.map_blocks(
            lambda x: cmap[x],
            x.data,
            dtype=cmap.dtype,
            new_axis=[x.data.ndim],
            chunks=x.chunks + (cmap.shape[1],),
            name=randomize("colorize"),
        )
    else:
        data = cmap[x.data]

    return xr.DataArray(data=data, dims=dims, coords=coords, attrs=attrs)
