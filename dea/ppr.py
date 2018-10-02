""" Parallel Processing tools
"""


def qmap(proc, q, eos_marker=None):
    """ Converts queue to an iterator.

    For every `item` in the `q` that is not `eos_marker`, `yield proc(item)`
    """
    while True:
        item = q.get(block=True)
        if item is eos_marker:
            q.task_done()
            break
        else:
            yield proc(item)
            q.task_done()


def q2q_map(proc, q_in, q_out, eos_marker=None):
    """ Like map but input and output are Queue objects.

    `eos_marker` - marks end of stream.
    """
    while True:
        item = q_in.get(block=True)
        if item is eos_marker:
            q_out.put(item, block=True)
            q_in.task_done()
            break
        else:
            q_out.put(proc(item))
            q_in.task_done()
