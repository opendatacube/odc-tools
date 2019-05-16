""" Notebook display helper methods.
"""
import numpy as np
import xarray as xr
from typing import Tuple, Optional, List


def guess_rgb_names(bands: List[str]) -> Tuple[str, str, str]:
    out = []
    for c in ('red', 'green', 'blue'):
        candidates = [name for name in bands if c in name]
        n = len(candidates)
        if n == 0:
            raise ValueError('Found no candidate for color "{}"'.format(c))
        elif n > 1:
            raise ValueError('Found too many candidates for color "{}"'.format(c))

        out.append(candidates[0])
    r, g, b = out
    return (r, g, b)


def to_rgba(ds: xr.Dataset,
            clamp: Optional[float] = None,
            bands: Optional[Tuple[str, str, str]] = None
            ) -> xr.DataArray:
    """ Given `xr.Dataset` with bands `red,green,blue` construct `xr.Datarray`
        containing uint8 rgba image.

    :param ds: xarray Dataset
    :param clamp: Value of the highest intensity value to use, if None, largest internsity value across all 3 channel is used.
    :param bands: Which bands to use, order should red,green,blue
    """

    if bands is None:
        bands = guess_rgb_names(list(ds.data_vars))

    r, g, b = (ds[name] for name in bands)
    nodata = r.nodata
    dims = r.dims + ('band',)

    r, g, b = (x.values for x in (r, g, b))
    a = (r != nodata).astype('uint8')*(0xFF)

    if clamp is None:
        clamp = max(x.max() for x in (r, g, b))

    r, g, b = ((np.clip(x, 0, clamp).astype('uint32')*255//clamp).astype('uint8')
               for x in (r, g, b))

    coords = dict(**{x.name: x.values
                     for x in ds.coords.values()},
                  band=['r', 'g', 'b', 'a'])

    attrs = {}
    crs = ds.attrs.get('crs')
    if crs is not None:
        attrs['crs'] = crs

    rgba = xr.DataArray(np.stack([r, g, b, a], axis=r.ndim),
                        coords=coords,
                        dims=dims,
                        attrs=attrs)

    return rgba


def image_shape(d):
    """ Returns (Height, Width) of a given dataset/datarray
    """
    dim_names = (('y', 'x'),
                 ('latitude', 'longitude'))

    dims = set(d.dims)
    h, w = None, None
    for n1, n2 in dim_names:
        if n1 in dims and n2 in dims:
            h, w = (d.coords[n].shape[0]
                    for n in (n1, n2))
            break

    if h is None:
        raise ValueError("Can't determine shape from dimension names: {}".format(' '.join(dims)))

    return (h, w)


def image_aspect(d):
    """ Given xarray Dataset|DataArray compute image aspect ratio
    """
    h, w = image_shape(d)
    return w/h


def mk_data_uri(data: bytes, mimetype: str = "image/png") -> str:
    from base64 import encodebytes
    return "data:{};base64,{}".format(mimetype, encodebytes(data).decode('ascii'))


def _to_png_data2(xx: np.ndarray, mode: str = 'auto') -> memoryview:
    from io import BytesIO
    import png

    if mode in ('auto', None):
        k = (2, 0) if xx.ndim == 2 else (xx.ndim, xx.shape[2])
        mode = {
            (2, 0): 'L',
            (2, 1): 'L',
            (3, 1): 'L',
            (3, 2): 'LA',
            (3, 3): 'RGB',
            (3, 4): 'RGBA'}.get(k, '')

        if mode == '':
            raise ValueError("Can't figure out mode automatically")

    bb = BytesIO()
    png.from_array(xx, mode).save(bb)
    return bb.getbuffer()


def _compress_image(im: np.ndarray,
                    driver='PNG',
                    **opts) -> bytes:
    import rasterio
    import warnings

    if im.dtype != np.uint8:
        raise ValueError("Only support uint8 images on input")

    if im.ndim == 3:
        h, w, nc = im.shape
        bands = np.transpose(im, axes=(2, 0, 1))
    elif im.ndim == 2:
        (h, w), nc = im.shape, 1
        bands = im.reshape(nc, h, w)
    else:
        raise ValueError('Expect 2 or 3 dimensional array got: {}'.format(im.ndim))

    rio_opts = dict(width=w,
                    height=h,
                    count=nc,
                    driver=driver,
                    dtype='uint8',
                    **opts)

    with warnings.catch_warnings():
        warnings.simplefilter('ignore', rasterio.errors.NotGeoreferencedWarning)

        with rasterio.MemoryFile() as mem:
            with mem.open(**rio_opts) as dst:
                dst.write(bands)
            return mem.read()


def to_png_data(im: np.ndarray, zlevel=6) -> bytes:
    return _compress_image(im, 'PNG', zlevel=zlevel)


def to_jpeg_data(im: np.ndarray, quality=95) -> bytes:
    return _compress_image(im, 'JPEG', quality=quality)


def xr_bounds(x, crs=None) -> Tuple[Tuple[float, float],
                                    Tuple[float, float]]:
    from datacube.utils.geometry import box
    from datacube.testutils.geom import epsg4326

    def get_range(a: np.ndarray) -> Tuple[float, float]:
        b = (a[1] - a[0])*0.5
        return a[0]-b, a[-1]+b

    if 'latitude' in x.coords:
        r1, r2 = (get_range(a.values) for a in (x.latitude, x.longitude))
        p1, p2 = ((r1[i], r2[i]) for i in (0, 1))
        return p1, p2

    if crs is None:
        crs = getattr(x, 'crs')
    if crs is None:
        raise ValueError('Need to supply CRS or use latitude/longitude coords')

    if not all(d in x.coords for d in crs.dimensions):
        raise ValueError('Incompatible CRS supplied')

    (t, b), (l, r) = (get_range(x.coords[dim].values)
                      for dim in crs.dimensions)

    l, b, r, t = box(l, b, r, t, crs).to_crs(epsg4326).boundingbox
    return ((t, r), (b, l))


def mk_image_overlay(xx: xr.Dataset,
                     clamp: Optional[float] = None,
                     bands: Optional[Tuple[str, str, str]] = None,
                     layer_name='Image',
                     fmt='png',
                     **opts):
    from ipyleaflet import ImageOverlay
    comp, mime = dict(
        png=(to_png_data, 'image/png'),
        jpg=(to_jpeg_data, 'image/jpeg'),
        jpeg=(to_jpeg_data, 'image/jpeg'),
    ).get(fmt.lower(), (None, None))

    if comp is None or mime is None:
        raise ValueError('Only support png an jpeg formats')

    if 'time' in xx.coords:
        nt = xx.time.shape[0]
        if nt == 1:
            xx = xx.isel(time=0)
        else:
            return [mk_image_overlay(xx.isel(time=t),
                                     clamp=clamp,
                                     bands=bands,
                                     layer_name="{}-{}".format(layer_name, t),
                                     fmt=fmt, **opts) for t in range(nt)]

    cc = to_rgba(xx, clamp=clamp, bands=bands)

    im_url = mk_data_uri(comp(cc.values, **opts), mime)
    return ImageOverlay(url=im_url,
                        bounds=xr_bounds(cc),
                        name=layer_name)
