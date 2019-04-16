""" Notebook display helper methods.
"""


def mk_cbk_ui():
    """ Create ipywidget and a callback to pass to `dc.load(progress_cbk=..)`
    """
    from ipywidgets import VBox, HBox, Label, Layout, IntProgress
    from timeit import default_timer as t_now

    pbar = IntProgress(min=0, max=100, value=0,
                       layout=Layout(width="100%"))
    lbl_right = Label("")
    lbl_left = Label("")
    info = HBox([lbl_left, lbl_right],
                layout=Layout(justify_content="space-between"))

    t0 = t_now()

    def cbk(n, ntotal):
        elapsed = t_now() - t0

        pbar.max = ntotal
        pbar.value = n

        lbl_right.value = "{:d} of {:d}".format(n, ntotal)
        lbl_left.value = "FPS: {:.1f}".format(n/elapsed)

    return VBox([info, pbar]), cbk


def with_ui_cbk():
    """ Use this inside notebook like so:

         dc.load(..., progress_cbk=with_ui_cbk())
    """
    from IPython.display import display
    ui, cbk = mk_cbk_ui()
    display(ui)
    return cbk


def simple_progress_cbk(n, total):
    print('\r{:4d} of {:4d}'.format(n, total), end='', flush=True)


def show_datasets(dss):
    from IPython.display import GeoJSON
    from datacube.testutils.geom import epsg4326
    return GeoJSON([ds.extent.to_crs(epsg4326).__geo_interface__ for ds in dss])
