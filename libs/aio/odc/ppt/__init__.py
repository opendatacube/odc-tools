""" Parallel Processing Tools
"""
from threading import BoundedSemaphore

EOS_MARKER = object()


def qmap(proc, q, eos_marker=None):
    """Converts queue to an iterator.

    For every `item` in the `q` that is not `eos_marker`, `yield proc(item)`
    """
    while True:
        item = q.get(block=True)
        if item is eos_marker:
            q.task_done()
            break
        else:
            try:
                yield proc(item)
            finally:
                q.task_done()


def q2q_map(proc, q_in, q_out, eos_marker=None):
    """Like map but input and output are Queue objects.

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


def future_results(it, max_active):
    """Given a generator of future objects return a generator of result tuples.

    Result Tuple contains:
     (fut.result(), None) -- if future succeeded
     (None, fut.exception()) -- if future failed


    This is roughly equivalent to:

        ((f.result(), f.exception()) for f in it)

    except that there are upto max_active futures that are active at any given
    time, and also order of result is "as completed". So it's like
    concurrent.futures.as_completed, but with upper bound on the number of
    active futures rather than a timeout.
    """
    import concurrent.futures as fut

    def result(f):
        err = f.exception()
        if err is not None:
            return (None, err)
        else:
            return (f.result(), None)

    def fill(src, n, dst):
        while n > 0:
            try:
                x = next(src)
            except StopIteration:
                return False

            dst.add(x)
            n -= 1
        return True

    max_fill = 10
    active = set()
    have_more = True

    try:
        while have_more:
            need_n = min(max_fill, max_active - len(active))
            have_more = fill(it, need_n, active)

            # blocking wait if active count is reached
            timeout = None if len(active) >= max_active else 0.01

            xx = fut.wait(active, timeout=timeout, return_when="FIRST_COMPLETED")
            active = xx.not_done

            for f in xx.done:
                yield result(f)
                if have_more:
                    have_more = fill(it, 1, active)

        # all futures were issued: do the flush now
        for f in fut.as_completed(active):
            yield result(f)
    finally:
        for f in active:
            f.cancel()


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
        s1.release()  # tell main thread we started
        x = action(*args, **kwargs)
        s2.acquire()  # wait for all threads to start
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
