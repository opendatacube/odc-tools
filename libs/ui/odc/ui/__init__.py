""" Notebook display helper methods.
"""
import numpy as np


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


def _ui_poll(f, sleep):
    from IPython import get_ipython
    import time
    import sys

    shell = get_ipython()
    kernel = shell.kernel
    events = []
    kernel.shell_handlers['execute_request'] = lambda *e: events.append(e)
    current_parent = (kernel._parent_ident, kernel._parent_header)
    shell.execution_count += 1

    def replay_events(shell, events):
        kernel = shell.kernel
        sys.stdout.flush()
        sys.stderr.flush()
        for stream, ident, parent in events:
            kernel.set_parent(ident, parent)
            kernel.execute_request(stream, ident, parent)
            if shell._last_traceback is not None:
                # there was an exception drop rest on the floor
                break

    def finalise():
        replay_events(shell, events)

    try:
        x = f()
        while x is None:
            kernel.do_one_iteration()
            kernel.set_parent(*current_parent)  # ensure stdout still happens in the same cell
            if sleep is not None:
                time.sleep(sleep)
            x = f()
        return x, finalise
    except Exception as e:
        raise e
    finally:
        kernel.shell_handlers['execute_request'] = kernel.execute_request


def ui_poll(f, sleep=0.02):
    import asyncio
    from warnings import warn

    x, finalise = _ui_poll(f, sleep)
    loop = asyncio.get_event_loop()
    if loop.is_running():
        loop.call_soon(finalise)
    else:
        warn('Automatic execution of scheduled cells only works with asyncio based ipython')

    return x


def dss_to_geojson(dss, bbox=False):
    from datacube.testutils.geom import epsg4326
    from datacube.utils.geometry import bbox_union

    geoms = [ds.extent.to_crs(epsg4326) for ds in dss]
    polygons = [g.__geo_interface__ for g in geoms]

    if bbox:
        return polygons, bbox_union(g.boundingbox for g in geoms)

    return polygons


def show_datasets(dss, mode='leaflet', **kw):
    if mode not in ('leaflet', 'geojson'):
        raise ValueError('Invalid value for mode, expected: leaflet|geojson')

    polygons, bbox = dss_to_geojson(dss, bbox=True)

    if mode == 'geojson':
        from IPython.display import GeoJSON
        return GeoJSON(polygons)
    if mode == 'leaflet':
        from ipyleaflet import Map, GeoJSON

        center = kw.pop('center', None)
        zoom = kw.pop('zoom', None)

        if center is None:
            center = (bbox.bottom + bbox.top)*0.5, (bbox.right + bbox.left)*0.5
        if zoom is None:
            zoom = 9  # TODO: auto compute zoom

        height = kw.pop('height', '600px')
        width = kw.pop('width', None)

        m = Map(center=center, zoom=zoom, **kw)
        m.layout.height = height
        m.layout.width = width

        gg = GeoJSON(data={'type': 'FeatureCollection',
                           'features': polygons},
                     hover_style={'color': 'tomato'})
        m.add_layer(gg)
        return m


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


def mk_map_region_selector(height='600px', **kwargs):
    from ipyleaflet import Map, WidgetControl, FullScreenControl, DrawControl
    from ipywidgets.widgets import Layout, Button, HTML
    from types import SimpleNamespace

    state = SimpleNamespace(selection=None,
                            bounds=None,
                            done=False)

    btn_done = Button(description='done',
                      layout=Layout(width='5em'))
    btn_done.style.button_color = 'green'
    btn_done.disabled = True

    html_info = HTML(layout=Layout(flex='1 0 20em',
                                   width='20em',
                                   height='3em'))

    def update_info(txt):
        html_info.value = '<pre style="color:grey">' + txt + '</pre>'

    m = Map(**kwargs) if len(kwargs) else Map(zoom=2)
    m.scroll_wheel_zoom = True
    m.layout.height = height

    draw = DrawControl()
    m.add_control(WidgetControl(widget=html_info, position='bottomleft'))
    m.add_control(WidgetControl(widget=btn_done, position='topright'))

    draw.circle = {}
    draw.polyline = {}
    draw.circlemarker = {}

    draw.rectangle = {"shapeOptions": {
        "fillColor": "#fca45d",
        "color": "#000000",
        "fillOpacity": 0.1
    }}
    draw.polygon = dict(**draw.rectangle)
    draw.edit = True
    draw.remove = True
    m.add_control(draw)
    m.add_control(FullScreenControl())

    def on_done(btn):
        state.done = True
        btn_done.disabled = True
        m.remove_control(draw)

    def bounds_handler(event):
        (lat1, lon1), (lat2, lon2) = event['new']
        txt = 'lat: [{:.{n}f}, {:.{n}f}]\nlon: [{:.{n}f}, {:.{n}f}]'.format(
            lat1, lat2, lon1, lon2, n=4)
        update_info(txt)
        state.bounds = dict(lat=(lat1, lat2),
                            lon=(lon1, lon2))

    def on_draw(event):
        v = event['new']
        action = event['name']
        if action == 'last_draw':
            state.selection = v['geometry']
        elif action == 'last_action' and v == 'deleted':
            state.selection = None

        btn_done.disabled = state.selection is None

    draw.observe(on_draw)
    m.observe(bounds_handler, ('bounds',))
    btn_done.on_click(on_done)

    return m, state


def select_on_a_map(**kwargs):
    from IPython.display import display
    m, state = mk_map_region_selector(**kwargs)
    display(m)
    return ui_poll(lambda: state.selection if state.done else None)
