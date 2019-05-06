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
