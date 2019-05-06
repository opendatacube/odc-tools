""" Interactive dc viewer
"""
from types import SimpleNamespace


def query_polygon(**kw):
    from datacube.api.query import Query
    return Query(**kw).geopolygon


class DcViewer():

    def __init__(self, dc,
                 time='2019-04',
                 height='600px',
                 out=None):
        self._dc = dc
        self._out = out
        products = list(p.name for p, c in dc.index.datasets.count_by_product())
        state, gui = self._build_ui(products, time, height=height)
        self._state = state
        self._gui = gui
        self._dss_layer = None
        self._dss = None
        self._last_query_bounds = None
        self._last_query_polygon = None

    def _build_ui(self,
                  product_names,
                  time,
                  height=None):
        from ipywidgets import widgets as w
        import ipyleaflet as L

        m = L.Map(zoom=2, scroll_wheel_zoom=True, layout=w.Layout(
            height=height
        ))
        m.add_control(L.FullScreenControl())

        prod_select = w.Dropdown(options=product_names, layout=w.Layout(
            flex='0 1 auto',
            width='10em',
        ))

        date_txt = w.Text(value=time, layout=w.Layout(
            flex='0 1 auto',
            width='6em',
        ))

        info_lbl = w.Label(value='', layout=w.Layout(
            flex='1 0 auto',
            # border='1px solid white',
        ))
        btn_show = w.Button(description='show', layout=w.Layout(
            flex='0 1 auto',
            width='4em',
        ), style=dict(
            button_color='green'
        ))

        ctrls = w.HBox([prod_select, w.Label('Time Period'), date_txt, info_lbl, btn_show],
                       layout=w.Layout(
                           border='1px solid tomato',
                       ))

        # m.add_control(L.WidgetControl(widget=ctrls, position='topright'))

        ui = w.VBox([ctrls, m], layout=w.Layout(
            border='2px solid plum',
        ))

        state = SimpleNamespace(time=time,
                                product=product_names[0],
                                count=0,
                                bounds=None)
        ui_state = SimpleNamespace(ui=ui,
                                   info=info_lbl,
                                   map=m)

        def bounds_handler(event):
            (lat1, lon1), (lat2, lon2) = event['new']
            lon1 = max(lon1, -180)
            lon2 = min(lon2, +180)
            lat1 = max(lat1, -90)
            lat2 = min(lat2, +90)

            state.bounds = dict(lat=(lat1, lat2),
                                lon=(lon1, lon2))

            self.on_bounds(state.bounds)

        def on_date_change(txt):
            state.time = txt.value
            self.on_date(state.time)

        def on_product_change(e):
            state.product = e['new']
            self.on_product(state.product)

        def on_show(b):
            state.time = date_txt.value
            self.on_show()

        date_txt.on_submit(on_date_change)
        prod_select.observe(on_product_change, ['value'])
        m.observe(bounds_handler, ('bounds',))
        btn_show.on_click(on_show)

        return state, ui_state

    def _update_info_count(self):
        from odc.index import dataset_count
        s = self._state
        spatial_query = s.bounds

        s.count = dataset_count(self._dc.index,
                                product=s.product,
                                time=s.time,
                                **spatial_query)
        self._gui.info.value = '{:,d} datasets in view'.format(s.count)

    def _clear_footprints(self):
        layer = self._dss_layer
        self._dss_layer = None

        if layer is not None:
            self._gui.map.remove_layer(layer)

    def _update_footprints(self):
        from . import show_datasets
        s = self._state
        dc = self._dc

        dss = dc.find_datasets(product=s.product,
                               time=s.time,
                               **s.bounds)
        self._dss = dss
        self._last_query_bounds = dict(**s.bounds)
        self._last_query_polygon = query_polygon(**s.bounds)

        if len(dss) > 0:
            new_layer = show_datasets(dss, dst=self._gui.map)
            self._clear_footprints()
            self._dss_layer = new_layer
        else:
            self._clear_footprints()

    def _maybe_show(self, max_dss, clear=False):
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
            self._maybe_show(500, clear=True)

    def on_date(self, time):
        self._update_info_count()
        self._maybe_show(500, clear=True)

    def on_show(self):
        self._update_footprints()

    def on_product(self, prod):
        self._update_info_count()
        self._maybe_show(500, clear=True)

    def _ipython_display_(self):
        return self._gui.ui._ipython_display_()
