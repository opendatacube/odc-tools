""" Notebook display helper methods (mapping related).
"""


def dss_to_geojson(dss, bbox=False, simplify=True, tolerance=0.001):
    from datacube.testutils.geom import epsg4326
    from datacube.utils.geometry import bbox_union

    geoms = [ds.extent.to_crs(epsg4326) for ds in dss]

    if simplify:
        geoms = [g.simplify(tolerance) for g in geoms]

    polygons = [g.__geo_interface__ for g in geoms]

    if bbox:
        return polygons, bbox_union(g.boundingbox for g in geoms)

    return polygons


def gridspec_to_geojson(gs, xx, yy, styles):
    """
    :param gs: GridSpec instance to render to GeoJSON
    :param xx: (x_start, x_end) in tile space
    :param yy: (y_start, y_end) in tile space
    """
    import itertools

    def to_geojson(gs, tidx):
        bbox = gs.tile_geobox(tidx)
        return dict(
            geometry=bbox.geographic_extent.__geo_interface__,
            type="Feature",
            properties=dict(title="{:+d},{:+d}".format(*tidx), **styles),
        )

    tiles = itertools.product(range(*xx), range(*yy))

    return {
        "type": "FeatureCollection",
        "features": [to_geojson(gs, tidx) for tidx in tiles],
    }


def zoom_from_bbox(bbox):
    """Estimate zoom level for a given bounding box region

    Bounding box is assumed to be in lat/lon.
    """
    import math

    x = max(360 / (bbox.right - bbox.left), 180 / (bbox.top - bbox.bottom))
    return math.floor(math.log2(x))


def show_datasets(
    dss,
    mode="leaflet",
    dst=None,
    layer_name="Datasets",
    style={},
    simplify=True,
    tolerance=0.001,  # ~100m at equator
    **kw
):
    """Display dataset footprints on a Leaflet map.

    :param mode:       leaflet|geojson, geojson only works in JupyterLab, leave it as default
    :param dst:        leaflet map to "show" datasets on, default -- create new one
    :param layer_name: name to give to the layer
    :param style:      GeoJSON style dictionary
        - weight
        - color/fillColor
        - opacity/fillOpacity
        - full list of options here: https://leafletjs.com/reference-1.5.0.html#path-option
    :param simplify:   simplify geometries before adding them to map
    :param tolerance:  tolerance in degrees for geometry simplification, default 0.001 ~ 111 meters at equator

    **kw: Arguments to pass to leaflet.Map(..) constructor
    """
    if mode not in ("leaflet", "geojson"):
        raise ValueError("Invalid value for mode, expected: leaflet|geojson")

    polygons, bbox = dss_to_geojson(
        dss, bbox=True, simplify=simplify, tolerance=tolerance
    )

    if mode == "geojson":
        from IPython.display import GeoJSON

        return GeoJSON(polygons)
    if mode == "leaflet":
        from ipyleaflet import Map, GeoJSON, FullScreenControl, LayersControl

        if dst is None:
            center = kw.pop("center", None)
            zoom = kw.pop("zoom", None)

            if center is None:
                center = (bbox.bottom + bbox.top) * 0.5, (bbox.right + bbox.left) * 0.5
            if zoom is None:
                zoom = zoom_from_bbox(bbox)

            height = kw.pop("height", "600px")
            width = kw.pop("width", None)

            m = Map(center=center, zoom=zoom, **kw)
            m.layout.height = height
            m.layout.width = width
            m.add_control(FullScreenControl())
            m.add_control(LayersControl())
        else:
            m = dst

        gg = GeoJSON(
            data={"type": "FeatureCollection", "features": polygons},
            style=style,
            hover_style={"color": "tomato"},
            name=layer_name,
        )
        m.add_layer(gg)
        if dst is None:
            return m
        else:
            return gg


def mk_map_region_selector(map=None, height="600px", **kwargs):
    from ipyleaflet import Map, WidgetControl, FullScreenControl, DrawControl
    from ipywidgets.widgets import Layout, Button, HTML
    from types import SimpleNamespace

    state = SimpleNamespace(selection=None, bounds=None, done=False)

    btn_done = Button(description="done", layout=Layout(width="5em"))
    btn_done.style.button_color = "green"
    btn_done.disabled = True

    html_info = HTML(layout=Layout(flex="1 0 20em", width="20em", height="3em"))

    def update_info(txt):
        html_info.value = '<pre style="color:grey">' + txt + "</pre>"

    def render_bounds(bounds):
        (lat1, lon1), (lat2, lon2) = bounds
        txt = "lat: [{:.{n}f}, {:.{n}f}]\nlon: [{:.{n}f}, {:.{n}f}]".format(
            lat1, lat2, lon1, lon2, n=4
        )
        update_info(txt)

    if map is None:
        m = Map(**kwargs) if len(kwargs) else Map(zoom=2)
        m.scroll_wheel_zoom = True
        m.layout.height = height
    else:
        m = map
        render_bounds(m.bounds)

    widgets = [
        WidgetControl(widget=btn_done, position="topright"),
        WidgetControl(widget=html_info, position="bottomleft"),
    ]
    for w in widgets:
        m.add_control(w)

    draw = DrawControl()

    draw.circle = {}
    draw.polyline = {}
    draw.circlemarker = {}

    shape_opts = {"fillColor": "#fca45d", "color": "#000000", "fillOpacity": 0.1}
    draw.rectangle = {"shapeOptions": shape_opts, "metric": ["km", "m"]}
    poly_opts = {"shapeOptions": dict(**shape_opts)}
    poly_opts["shapeOptions"]["original"] = dict(**shape_opts)
    poly_opts["shapeOptions"]["editing"] = dict(**shape_opts)

    draw.polygon = poly_opts
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
        bounds = event["new"]
        render_bounds(bounds)
        (lat1, lon1), (lat2, lon2) = bounds
        state.bounds = dict(lat=(lat1, lat2), lon=(lon1, lon2))

    def on_draw(event):
        v = event["new"]
        action = event["name"]
        if action == "last_draw":
            state.selection = v["geometry"]
        elif action == "last_action" and v == "deleted":
            state.selection = None

        btn_done.disabled = state.selection is None

    draw.observe(on_draw)
    m.observe(bounds_handler, ("bounds",))
    btn_done.on_click(on_done)

    return m, state


def select_on_a_map(map=None, **kwargs):
    """Display a map and block execution until user selects a region of interest.

    Returns selected region as datacube Geometry class.

        polygon = select_on_map()

    **kwargs**
      map    -- Instead of creating new map use existing Ipyleaflet map
      height -- height of the map, for example "500px", "10el"

    Any parameter ipyleaflet.Map(..) accepts:
      zoom   Int
      center (lat: Float, lon: Float)
      ...
    """
    from IPython.display import display
    from ._ui import ui_poll

    m, state = mk_map_region_selector(map=map, **kwargs)
    if map is None:
        display(m)

    def extract_geometry(state):
        from datacube.utils.geometry import Geometry
        from datacube.testutils.geom import epsg4326

        return Geometry(state.selection, epsg4326)

    return ui_poll(lambda: extract_geometry(state) if state.done else None)
