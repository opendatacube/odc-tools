""" Notebook display helper methods.
"""


def mk_cbk_ui(width='100%'):
    """ Create ipywidget and a callback to pass to `dc.load(progress_cbk=..)`

        :param width: Width of the UI, for example: '80%' '200px' '30em'
    """
    from ipywidgets import VBox, HBox, Label, Layout, IntProgress
    from timeit import default_timer as t_now

    pbar = IntProgress(min=0, max=100, value=0,
                       layout=Layout(width='100%'))
    lbl_right = Label("")
    lbl_left = Label("")
    info = HBox([lbl_left, lbl_right],
                layout=Layout(justify_content="space-between"))

    ui = VBox([info, HBox([pbar])],
              layout=Layout(width=width))

    t0 = t_now()

    def cbk(n, ntotal):
        elapsed = t_now() - t0

        pbar.max = ntotal
        pbar.value = n

        lbl_right.value = "{:d} of {:d}".format(n, ntotal)
        lbl_left.value = "FPS: {:.1f}".format(n/elapsed)

    return ui, cbk


def with_ui_cbk(width='100%', **kwargs):
    """ Use this inside notebook like so:

         dc.load(..., progress_cbk=with_ui_cbk())

        :param width: Width of the UI, for example: '80%' '200px' '30em'
    """
    from IPython.display import display
    ui, cbk = mk_cbk_ui(width=width, **kwargs)
    display(ui)
    return cbk


def simple_progress_cbk(n, total):
    print('\r{:4d} of {:4d}'.format(n, total), end='', flush=True)


def show_datasets(dss):
    from IPython.display import GeoJSON
    from datacube.testutils.geom import epsg4326
    return GeoJSON([ds.extent.to_crs(epsg4326).__geo_interface__ for ds in dss])


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
