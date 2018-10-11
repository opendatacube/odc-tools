""" Parallel Processing tools
"""
from threading import BoundedSemaphore


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


def pool_broadcast(pool, action, *args, **kwargs):
    """Broadcast action across thread pool.

       Will submit action(*args, **kwargs) N times to thread pool, making sure
       that no thread's action finishes before all other actions have started,
       hence forcing each incarnation of `action` to run in it's own thread.

       This function waits for all of these to complete and returns a list of results.
    """
    N = pool._max_workers

    s1 = BoundedSemaphore(N)
    s2 = BoundedSemaphore(N)

    def bcast_action():
        s1.release()                 # tell main thread we started
        x = action(*args, **kwargs)
        s2.acquire()                 # wait for all threads to start
        return x

    for _ in range(N):
        s1.acquire()
        s2.acquire()

    rr = [pool.submit(bcast_action) for _ in range(N)]

    # wait for all to start
    for _ in range(N):
        s1.acquire()

    # allow all to continue
    for _ in range(N):
        s2.release()

    # Extract results (this might block if action is long running)
    # TODO: deal with possible failures
    return [r.result() for r in rr]
