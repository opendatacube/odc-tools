""" Notebook display helper methods (mapping related).
"""

# pylint:disable=import-outside-toplevel

import math


def dss_to_geojson(dss, bbox=False, simplify=True, tolerance=0.001):
    from odc.geo.testutils import epsg4326
    from odc.geo.geom import bbox_union

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
        return {
            "geometry": bbox.geographic_extent.__geo_interface__,
            "type": "Feature",
            "properties": {
                # pylint:disable=consider-using-f-string
                "title": "{:+d},{:+d}".format(*tidx),
                **styles,
            },
        }

    tiles = itertools.product(range(*xx), range(*yy))

    return {
        "type": "FeatureCollection",
        "features": [to_geojson(gs, tidx) for tidx in tiles],
    }


def zoom_from_bbox(bbox):
    """Estimate zoom level for a given bounding box region

    Bounding box is assumed to be in lat/lon.
    """
    x = max(360 / (bbox.right - bbox.left), 180 / (bbox.top - bbox.bottom))
    return math.floor(math.log2(x))


def show_datasets(
    dss,
    mode="leaflet",
    dst=None,
    layer_name="Datasets",
    style=None,
    simplify=True,
    tolerance=0.001,  # ~100m at equator
    **kw,
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
    if style is None:
        style = {}
    if mode not in ("leaflet", "geojson"):
        raise ValueError("Invalid value for mode, expected: leaflet|geojson")

    polygons, bbox = dss_to_geojson(
        dss, bbox=True, simplify=simplify, tolerance=tolerance
    )

    if mode == "geojson":
        from IPython.display import GeoJSON

        return GeoJSON(polygons)
    if mode == "leaflet":
        from ipyleaflet import FullScreenControl, GeoJSON, LayersControl, Map

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
            m.add(FullScreenControl())
            m.add(LayersControl())
        else:
            m = dst

        gg = GeoJSON(
            data={"type": "FeatureCollection", "features": polygons},
            style=style,
            hover_style={"color": "tomato"},
            name=layer_name,
        )
        m.add(gg)
        if dst is None:
            return m
        else:
            return gg
    return None


def mk_map_region_selector(m=None, height="600px", **kwargs):
    from types import SimpleNamespace

    from ipyleaflet import DrawControl, FullScreenControl, Map, WidgetControl
    from ipywidgets.widgets import HTML, Button, Layout

    state = SimpleNamespace(selection=None, bounds=None, done=False)

    btn_done = Button(description="done", layout=Layout(width="5em"))
    btn_done.style.button_color = "green"
    btn_done.disabled = True

    html_info = HTML(layout=Layout(flex="1 0 20em", width="20em", height="3em"))

    def update_info(txt):
        html_info.value = '<pre style="color:grey">' + txt + "</pre>"

    def render_bounds(bounds):
        (lat1, lon1), (lat2, lon2) = bounds
        txt = f"lat: [{lat1:.{4}f}, {lat2:.{4}f}]\nlon: [{lon1:.{4}f}, {lon2:.{4}f}]"
        update_info(txt)

    if m is None:
        m = Map(**kwargs) if len(kwargs) else Map(zoom=2)
        m.scroll_wheel_zoom = True
        m.layout.height = height
    else:
        render_bounds(m.bounds)

    widgets = [
        WidgetControl(widget=btn_done, position="topright"),
        WidgetControl(widget=html_info, position="bottomleft"),
    ]
    for w in widgets:
        m.add(w)

    draw = DrawControl()

    draw.circle = {}
    draw.polyline = {}
    draw.circlemarker = {}

    shape_opts = {"fillColor": "#fca45d", "color": "#000000", "fillOpacity": 0.1}
    draw.rectangle = {"shapeOptions": shape_opts, "metric": ["km", "m"]}
    poly_opts = {"shapeOptions": {**shape_opts}}
    poly_opts["shapeOptions"]["original"] = {**shape_opts}
    poly_opts["shapeOptions"]["editing"] = {**shape_opts}

    draw.polygon = poly_opts
    draw.edit = True
    draw.remove = True
    m.add(draw)
    m.add(FullScreenControl())

    def on_done(btn):
        state.done = True
        btn_done.disabled = True
        m.remove(draw)
        for w in widgets:
            m.remove(w)

    def bounds_handler(event):
        bounds = event["new"]
        render_bounds(bounds)
        (lat1, lon1), (lat2, lon2) = bounds
        state.bounds = {"lat": (lat1, lat2), "lon": (lon1, lon2)}

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


def select_on_a_map(m=None, **kwargs):
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

    m_2, state = mk_map_region_selector(m=m, **kwargs)
    if m is None:
        display(m_2)

    def extract_geometry(state):
        from odc.geo.testutils import epsg4326
        from odc.geo.geom import Geometry

        return Geometry(state.selection, epsg4326)

    return ui_poll(lambda: extract_geometry(state) if state.done else None)
