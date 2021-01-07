""" Notebook display helper methods.
"""


def mk_cbk_ui(width="100%"):
    """Create ipywidget and a callback to pass to `dc.load(progress_cbk=..)`

    :param width: Width of the UI, for example: '80%' '200px' '30em'
    """
    from ipywidgets import VBox, HBox, Label, Layout, IntProgress
    from timeit import default_timer as t_now

    pbar = IntProgress(min=0, max=100, value=0, layout=Layout(width="100%"))
    lbl_right = Label("")
    lbl_left = Label("")
    info = HBox([lbl_left, lbl_right], layout=Layout(justify_content="space-between"))

    ui = VBox([info, HBox([pbar])], layout=Layout(width=width))

    t0 = t_now()

    def cbk(n, ntotal):
        elapsed = t_now() - t0

        pbar.max = ntotal
        pbar.value = n

        lbl_right.value = "{:d} of {:d}".format(n, ntotal)
        lbl_left.value = "FPS: {:.1f} ({:0.1f} s remaining)".format(
            n / elapsed, elapsed / n * (ntotal - n)
        )

    return ui, cbk


def with_ui_cbk(width="100%", **kwargs):
    """Use this inside notebook like so:

     dc.load(..., progress_cbk=with_ui_cbk())

    :param width: Width of the UI, for example: '80%' '200px' '30em'
    """
    from IPython.display import display

    ui, cbk = mk_cbk_ui(width=width, **kwargs)
    display(ui)
    return cbk


def simple_progress_cbk(n, total):
    print("\r{:4d} of {:4d}".format(n, total), end="", flush=True)


def ui_poll(f, sleep=0.02, n=1):
    from jupyter_ui_poll import run_ui_poll_loop

    return run_ui_poll_loop(f, sleep, n=n)
