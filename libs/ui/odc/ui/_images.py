""" Notebook display helper methods.
"""

import numpy as np
import rasterio
import warnings
import xarray as xr
from base64 import encodebytes
from ipyleaflet import ImageOverlay
from odc.algo import is_rgb, to_rgba
from typing import Optional, Tuple, Union

from datacube.utils.geometry import box
from datacube.testutils.geom import epsg4326


def image_shape(d):
    """Returns (Height, Width) of a given dataset/datarray"""
    dim_names = (("y", "x"), ("latitude", "longitude"))

    dims = set(d.dims)
    h, w = None, None
    for n1, n2 in dim_names:
        if n1 in dims and n2 in dims:
            h, w = (d.coords[n].shape[0] for n in (n1, n2))
            break

    if h is None:
        raise ValueError(
            f"Can't determine shape from dimension names: {' '.join(dims)}"
        )

    return h, w


def image_aspect(d):
    """Given xarray Dataset|DataArray compute image aspect ratio"""
    h, w = image_shape(d)
    return w / h


def replace_transparent_pixels(
    rgba: np.ndarray, color: Tuple[int, int, int] = (255, 0, 255)
) -> np.ndarray:
    """
    RGBA -> RGB with transparent pixels replaced with given color
    """
    assert rgba.ndim == 3
    assert rgba.shape[-1] == 4

    m = rgba[..., -1] == 0
    rgb = rgba[..., :3].copy()
    rgb[m] = color
    return rgb


def mk_data_uri(data: bytes, mimetype: str = "image/png") -> str:
    return f"data:{mimetype};base64,{encodebytes(data).decode('ascii')}"


def _compress_image(im: np.ndarray, driver="PNG", **opts) -> bytes:
    if im.dtype != np.uint8:
        raise ValueError("Only support uint8 images on input")

    if im.ndim == 3:
        h, w, nc = im.shape
        bands = np.transpose(im, axes=(2, 0, 1))
    elif im.ndim == 2:
        (h, w), nc = im.shape, 1
        bands = im.reshape(nc, h, w)
    else:
        raise ValueError(f"Expect 2 or 3 dimensional array got: {im.ndim}")

    rio_opts = {
        "width": w,
        "height": h,
        "count": nc,
        "driver": driver,
        "dtype": "uint8",
        **opts,
    }

    with warnings.catch_warnings():
        warnings.simplefilter("ignore", rasterio.errors.NotGeoreferencedWarning)

        with rasterio.MemoryFile() as mem:
            with mem.open(**rio_opts) as dst:
                dst.write(bands)
            return mem.read()


def to_png_data(im: np.ndarray, zlevel=6) -> bytes:
    return _compress_image(im, "PNG", zlevel=zlevel)


def to_jpeg_data(im: np.ndarray, quality=95, transparent=None) -> bytes:
    if transparent is not None:
        im = replace_transparent_pixels(im, transparent)
    return _compress_image(im, "JPEG", quality=quality)


def xr_bounds(x, crs=None) -> Tuple[Tuple[float, float], Tuple[float, float]]:
    def get_range(a: np.ndarray) -> Tuple[float, float]:
        b = (a[1] - a[0]) * 0.5
        return a[0] - b, a[-1] + b

    if "latitude" in x.coords:
        r1, r2 = (get_range(a.values) for a in (x.latitude, x.longitude))
        p1, p2 = ((r1[i], r2[i]) for i in (0, 1))
        return p1, p2

    if crs is None:
        geobox = getattr(x, "geobox", None)
        if geobox:
            crs = geobox.crs

    if crs is None:
        raise ValueError("Need to supply CRS or use latitude/longitude coords")

    if not all(d in x.coords for d in crs.dimensions):
        raise ValueError("Incompatible CRS supplied")

    (t, b), (l, r) = (get_range(x.coords[dim].values) for dim in crs.dimensions)

    l, b, r, t = box(l, b, r, t, crs).to_crs(epsg4326).boundingbox
    return (t, r), (b, l)


def mk_image_overlay(
    xx: Union[xr.Dataset, xr.DataArray],
    clamp: Optional[float] = None,
    bands: Optional[Tuple[str, str, str]] = None,
    layer_name="Image",
    fmt="png",
    **opts,
):
    """Create ipyleaflet.ImageLayer from raster data.

    xx - xarray.Dataset that will be converted to RGBA or
         xarray.DataArray that is already in RGB(A) format

    clamp, bands -- passed on to to_rgba(..), only used when xx is xarray.Dataset

    Returns
    =======
    ipyleaflet.ImageOverlay or a list of them one per time slice
    """

    comp, mime = {
        "png": (to_png_data, "image/png"),
        "jpg": (to_jpeg_data, "image/jpeg"),
        "jpeg": (to_jpeg_data, "image/jpeg"),
    }.get(fmt.lower(), (None, None))

    if comp is None or mime is None:
        raise ValueError("Only support png an jpeg formats")

    if "time" in xx.dims:
        nt = xx.time.shape[0]
        if nt == 1:
            xx = xx.isel(time=0)
        else:
            return [
                mk_image_overlay(
                    xx.isel(time=t),
                    clamp=clamp,
                    bands=bands,
                    layer_name="{layer_name}-{t}",
                    fmt=fmt,
                    **opts,
                )
                for t in range(nt)
            ]

    if isinstance(xx, xr.Dataset):
        rgba = to_rgba(xx, clamp=clamp, bands=bands)
    else:
        if not is_rgb(xx):
            raise ValueError("Expect RGB xr.DataArray")
        rgba = xx

    im_url = mk_data_uri(comp(rgba.values, **opts), mime)
    return ImageOverlay(url=im_url, bounds=xr_bounds(rgba), name=layer_name)
