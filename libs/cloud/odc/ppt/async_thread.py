"""Tools for working with async tasks
"""
import asyncio
import logging
import threading

from . import EOS_MARKER

log = logging.getLogger(__name__)


class AsyncThread(object):
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
