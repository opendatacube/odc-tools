""" Notebook display helper methods.
"""
import numpy as np


def to_rgba(ds,
            clamp=None,
            bands=('red', 'green', 'blue')):
    """ Given `xr.Dataset` with bands `red,green,blue` construct `xr.Datarray`
        containing uint8 rgba image.

    :param ds: xarray Dataset
    :param clamp: Value of the highest intensity value to use, if None, largest internsity value across all 3 channel is used.
    :param bands: Which bands to use, order should red,green,blue
    """
    import numpy as np
    import xarray as xr

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
    rgba = xr.DataArray(np.stack([r, g, b, a], axis=r.ndim),
                        coords=coords,
                        dims=dims)

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


def _to_png_data2(xx: np.ndarray, mode: str = 'auto') -> bytes:
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
            (3, 4): 'RGBA'}.get(k, None)

        if mode is None:
            raise ValueError("Can't figure out mode automatically")

    bb = BytesIO()
    png.from_array(xx, mode).save(bb)
    return bb.getbuffer()


def to_png_data(im: np.ndarray) -> bytes:
    import rasterio
    import warnings

    if im.dtype != np.uint8:
        raise ValueError("Only support uint8 images on input")

    if im.ndim == 3:
        h, w, nc = im.shape
        bands = np.transpose(im, axes=(2, 0, 1))
    elif im.ndim == 2:
        h, w, nc = (*im.shape, 1)
        bands = im.reshape(nc, h, w)
    else:
        raise ValueError('Expect 2 or 3 dimensional array got: {}'.format(im.ndim))

    rio_opts = dict(width=w,
                    height=h,
                    count=nc,
                    driver='PNG',
                    dtype='uint8')

    with warnings.catch_warnings():
        warnings.simplefilter('ignore', rasterio.errors.NotGeoreferencedWarning)

        with rasterio.MemoryFile() as mem:
            with mem.open(**rio_opts) as dst:
                dst.write(bands)
            return mem.read()


def xr_bounds(x):
    def get_range(a):
        b = (a[1] - a[0])*0.5
        return a[0]-b, a[-1]+b
    if 'latitude' in x.coords:
        r1, r2 = (get_range(a.values) for a in (x.latitude, x.longitude))
        return tuple((r1[i], r2[i]) for i in (0, 1))
    else:
        raise ValueError('Needs to have latitude/longitude coordinates')


def mk_image_overlay(xx,
                     clamp=3000,
                     bands=('red', 'green', 'blue'),
                     layer_name='Image'):
    from ipyleaflet import ImageOverlay
    cc = to_rgba(xx, clamp, bands)
    im_url = mk_data_uri(to_png_data(cc.values))
    return ImageOverlay(url=im_url,
                        bounds=xr_bounds(cc),
                        name=layer_name)
