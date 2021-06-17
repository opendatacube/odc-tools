""" Interactive dc viewer
"""
from types import SimpleNamespace
from pandas import Period
from datacube.api.query import Query
from odc.index import dataset_count
from ._map import show_datasets


def query_polygon(**kw):
    return Query(**kw).geopolygon


def dt_step(d: str, step: int = 1) -> str:
    return str(Period(d) + step)


class DcViewer:
    def __init__(
        self,
        dc,
        time,
        products=None,
        zoom=None,
        center=None,
        height=None,
        width=None,
        style=None,
        max_datasets_to_display=2000,
    ):
        """
        :param dc:        Datacube object
        :param time:      Initial time as a string
        :param products:  None -- all products, 'non-empty' all non empty products, or a list or product names
        :param zoom:      Initial zoom factor for a map
        :param center:    Initial center for a map in Lat,Lon
        :param height:    Example: '600px', '10em'
        :param width:     Example: '100%', '600px'
        :param style:     Dictionary for styling for dataset footprint
            - weight
            - color/fillColor
            - opacity/fillOpacity
            - full list of options here: https://leafletjs.com/reference-1.5.0.html#path-option
        :param max_datasets_to_display: If the view contains more than that many datasets don't bother querying DB
                                        and displaying footprints.
        """
        self._dc = dc

        if products is None:
            products = [p.name for p in dc.index.products.get_all()]
        elif products == "non-empty":
            products = list(p.name for p, c in dc.index.datasets.count_by_product())

        if style is None:
            style = dict(fillOpacity=0.1, weight=1)

        state, gui = self._build_ui(
            products, time, zoom=zoom, center=center, height=height, width=width
        )
        self._state = state
        self._gui = gui
        self._dss_layer = None
        self._dss = None
        self._last_query_bounds = None
        self._last_query_polygon = None
        self._style = style
        self._max_datasets_to_display = max_datasets_to_display

    def _build_ui(
        self, product_names, time, zoom=None, center=None, height=None, width=None
    ):
        from ipywidgets import widgets as w
        import ipyleaflet as L

        pp = {"zoom": zoom or 1}

        if center is not None:
            pp["center"] = center

        m = L.Map(**pp, scroll_wheel_zoom=True)
        m.add_control(L.FullScreenControl())

        prod_select = w.Dropdown(
            options=product_names,
            layout=w.Layout(
                flex="0 1 auto",
                width="10em",
            ),
        )

        date_txt = w.Text(
            value=time,
            layout=w.Layout(
                flex="0 1 auto",
                width="6em",
            ),
        )

        info_lbl = w.Label(
            value="",
            layout=w.Layout(
                flex="1 0 auto",
                # border='1px solid white',
            ),
        )
        btn_bwd = w.Button(
            icon="step-backward",
            layout=w.Layout(
                flex="0 1 auto",
                width="3em",
            ),
        )
        btn_fwd = w.Button(
            icon="step-forward",
            layout=w.Layout(
                flex="0 1 auto",
                width="3em",
            ),
        )
        btn_show = w.Button(
            description="show",
            layout=w.Layout(
                flex="0 1 auto",
                width="6em",
            ),
            style=dict(
                # button_color='green'
            ),
        )

        ctrls = w.HBox(
            [
                prod_select,
                w.Label("Time Period"),
                date_txt,
                btn_bwd,
                btn_fwd,
                info_lbl,
                btn_show,
            ],
            layout=w.Layout(
                # border='1px solid tomato',
            ),
        )
        # m.add_control(L.WidgetControl(widget=ctrls, position='topright'))

        ui = w.VBox(
            [ctrls, m],
            layout=w.Layout(
                width=width,
                height=height,
                # border='2px solid plum',
            ),
        )

        state = SimpleNamespace(
            time=time, product=product_names[0], count=0, bounds=None
        )
        ui_state = SimpleNamespace(ui=ui, info=info_lbl, map=m)

        def bounds_handler(event):
            (lat1, lon1), (lat2, lon2) = event["new"]
            lon1 = max(lon1, -180)
            lon2 = min(lon2, +180)
            lat1 = max(lat1, -90)
            lat2 = min(lat2, +90)

            state.bounds = dict(lat=(lat1, lat2), lon=(lon1, lon2))

            self.on_bounds(state.bounds)

        def on_date_change(txt):
            state.time = txt.value
            self.on_date(state.time)

        def on_product_change(e):
            state.product = e["new"]
            self.on_product(state.product)

        def on_show(b):
            state.time = date_txt.value
            self.on_show()

        def time_advance(step):
            date_txt.value = dt_step(date_txt.value, step)
            on_date_change(date_txt)

        date_txt.on_submit(on_date_change)
        prod_select.observe(on_product_change, ["value"])
        m.observe(bounds_handler, ("bounds",))
        btn_show.on_click(on_show)
        btn_fwd.on_click(lambda b: time_advance(1))
        btn_bwd.on_click(lambda b: time_advance(-1))

        return state, ui_state

    def _update_info_count(self):
        s = self._state
        spatial_query = s.bounds

        s.count = dataset_count(
            self._dc.index, product=s.product, time=s.time, **spatial_query
        )
        self._gui.info.value = "{:,d} datasets in view".format(s.count)

    def _clear_footprints(self):
        layer = self._dss_layer
        self._dss_layer = None
        self._last_query_bounds = None
        self._last_query_polygon = None

        if layer is not None:
            self._gui.map.remove_layer(layer)

    def _update_footprints(self):
        s = self._state
        dc = self._dc

        dss = dc.find_datasets(product=s.product, time=s.time, **s.bounds)
        self._dss = dss

        if len(dss) > 0:
            new_layer = show_datasets(dss, dst=self._gui.map, style=self._style)
            self._clear_footprints()

            self._dss_layer = new_layer
            self._last_query_bounds = dict(**s.bounds)
            self._last_query_polygon = query_polygon(**s.bounds)
        else:
            self._clear_footprints()

    def _maybe_show(self, max_dss=None, clear=False):
        if max_dss is None:
            max_dss = self._max_datasets_to_display

        if self._state.count < max_dss:
            self._update_footprints()
        elif clear:
            self._clear_footprints()

    def on_bounds(self, bounds):
        self._update_info_count()
        skip_refresh = False
        if self._last_query_polygon is not None:
            if self._last_query_polygon.contains(query_polygon(**bounds)):
                skip_refresh = True

        if not skip_refresh:
            self._maybe_show(clear=True)

    def on_date(self, time):
        self._update_info_count()
        self._maybe_show(clear=True)

    def on_show(self):
        self._update_footprints()

    def on_product(self, prod):
        self._update_info_count()
        self._maybe_show(clear=True)

    def _ipython_display_(self):
        return self._gui.ui._ipython_display_()
