""" Parallel Processing Tools
"""

import asyncio
import logging
import threading
import concurrent.futures as fut
from threading import BoundedSemaphore

log = logging.getLogger(__name__)
EOS_MARKER = object()

# pylint: disable=consider-using-with


def qmap(proc, q, eos_marker=None):
    """Converts queue to an iterator.

    For every `item` in the `q` that is not `eos_marker`, `yield proc(item)`
    """
    while True:
        item = q.get(block=True)
        if item is eos_marker:
            q.task_done()
            break

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

    def result(f):
        err = f.exception()
        if err is not None:
            return None, err
        else:
            return f.result(), None

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


class AsyncThread:
    @staticmethod
    def _worker(loop):
        asyncio.set_event_loop(loop)
        loop.run_forever()
        loop.close()

    def __init__(self):
        self._loop = asyncio.new_event_loop()
        self._thread = threading.Thread(target=AsyncThread._worker, args=(self._loop,))
        self._thread.start()

    def terminate(self):
        def _stop(loop):
            loop.stop()

        if self._loop is None:
            return

        self.call_soon(_stop, self._loop)
        self._thread.join()
        self._loop, self._thread = None, None

    def __del__(self):
        self.terminate()

    def submit(self, func, *args, **kwargs):
        """Run async func with args/kwargs in separate thread, returns Future object."""
        return asyncio.run_coroutine_threadsafe(func(*args, **kwargs), self._loop)

    def call_soon(self, func, *args):
        """Call normal (non-async) function with arguments in the processing thread
        it's just a wrapper over `loop.call_soon_threadsafe()`

        Returns a handle with `.cancel`, not a full on Future
        """
        return self._loop.call_soon_threadsafe(func, *args)

    @property
    def loop(self):
        return self._loop

    def from_queue(self, q, eos_marker=EOS_MARKER):
        """Convert qsync queue to a sync iterator,

        yield items from async queue until eos_marker is observed

        Queue's loop have to be the same as self.loop
        """

        async def drain_q(q):
            def get(q):
                x = q.get_nowait()
                q.task_done()
                return x

            return [get(q) for _ in range(q.qsize())]

        while True:
            xx = self.submit(drain_q, q).result()
            for x in xx:
                if x is eos_marker:
                    return
                yield x
