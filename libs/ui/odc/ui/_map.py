""" Notebook display helper methods (mapping related).
"""


def dss_to_geojson(dss, bbox=False):
    from datacube.testutils.geom import epsg4326
    from datacube.utils.geometry import bbox_union

    geoms = [ds.extent.to_crs(epsg4326) for ds in dss]
    polygons = [g.__geo_interface__ for g in geoms]

    if bbox:
        return polygons, bbox_union(g.boundingbox for g in geoms)

    return polygons


def zoom_from_bbox(bbox):
    """ Estimate zoom level for a given bounding box region

        Bounding box is assumed to be in lat/lon.
    """
    import math
    x = max(360/(bbox.right - bbox.left),
            180/(bbox.top - bbox.bottom))
    return math.floor(math.log2(x))


def show_datasets(dss, mode='leaflet', dst=None, **kw):
    if mode not in ('leaflet', 'geojson'):
        raise ValueError('Invalid value for mode, expected: leaflet|geojson')

    polygons, bbox = dss_to_geojson(dss, bbox=True)

    if mode == 'geojson':
        from IPython.display import GeoJSON
        return GeoJSON(polygons)
    if mode == 'leaflet':
        from ipyleaflet import Map, GeoJSON

        if dst is None:
            center = kw.pop('center', None)
            zoom = kw.pop('zoom', None)

            if center is None:
                center = (bbox.bottom + bbox.top)*0.5, (bbox.right + bbox.left)*0.5
            if zoom is None:
                zoom = zoom_from_bbox(bbox)

            height = kw.pop('height', '600px')
            width = kw.pop('width', None)

            m = Map(center=center, zoom=zoom, **kw)
            m.layout.height = height
            m.layout.width = width
        else:
            m = dst

        gg = GeoJSON(data={'type': 'FeatureCollection',
                           'features': polygons},
                     hover_style={'color': 'tomato'})
        m.add_layer(gg)
        if dst is None:
            return m
        else:
            return gg


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

    widgets = [
        WidgetControl(widget=btn_done, position='topright'),
        WidgetControl(widget=html_info, position='bottomleft'),
    ]
    for w in widgets:
        m.add_control(w)

    draw = DrawControl()

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
        for w in widgets:
            m.remove_control(w)

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
    from ._ui import ui_poll

    m, state = mk_map_region_selector(**kwargs)
    display(m)

    def extract_geometry(state):
        from datacube.utils.geometry import Geometry
        from datacube.testutils.geom import epsg4326

        return Geometry(state.selection, epsg4326)

    return ui_poll(lambda: extract_geometry(state) if state.done else None)
