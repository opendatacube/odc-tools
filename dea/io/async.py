"""Tools for working with async tasks
"""
import asyncio
import queue
import itertools
import logging
import threading
from types import SimpleNamespace
from concurrent.futures import ThreadPoolExecutor
from ..ppr import pool_broadcast


EOS_MARKER = object()
log = logging.getLogger(__name__)


async def async_q2q_map(func, q_in, q_out,
                        eos_marker=EOS_MARKER,
                        eos_passthrough=True,
                        **kwargs):
    """Like `map` but operating on values from/to queues.

       Roughly equivalent to:

       > while not end of stream:
       >    q_out.put(func(q_in.get(), **kwargs))

       Processing stops when `eos_marker` object is observed on input, by
       default `eos_marker` is passed through to output queue, but you can
       disable that.

       Calls `task_done()` method on input queue after result was copied to output queue.

       Assumption is that mapping function doesn't raise exceptions, instead it
       should return some sort of error object. If calling `func` does result
       in an exception it will be caught and logged but otherwise ignored.

       It is safe to have multiple consumers/producers reading/writing from the
       queues, although you might want to disable eos pass-through in those
       cases.

       func : Callable

       q_in: Input asyncio.Queue
       q_out: Output asyncio.Queue

       eos_marker: Value that indicates end of stream

       eos_passthrough: If True copy eos_marker to output queue before
                        terminating, if False then don't

    """
    while True:
        x = await q_in.get()

        if x is eos_marker:
            if eos_passthrough:
                await q_out.put(x)
            q_in.task_done()
            return

        err, result = (None, None)
        try:
            result = await func(x, **kwargs)
        except Exception as e:
            err = str(e)
            log.error("Uncaught exception: %s", err)

        if err is None:
            await q_out.put(result)

        q_in.task_done()


async def gen2q_async(func,
                      q_out,
                      nconcurrent,
                      eos_marker=EOS_MARKER,
                      eos_passthrough=True,
                      loop=None):
    """ Run upto `nconcurrent` generator functions, pump values from generator function into `q_out`
        To indicate that no more data is available func should return special value `eos_marker`

          [func(0)] \
          [func(1)]  >--> q_out
          [func(2)] /

        - func is expected not to raise exceptions
    """

    async def worker(idx):
        n = 0
        while True:
            try:
                x = await func(idx)
            except Exception as e:
                log.error("Uncaught exception: %s", str(e))
                return n

            if x is eos_marker:
                return n
            n += 1
            await q_out.put(x)
        return n

    ff = [asyncio.ensure_future(worker(i), loop=loop)
          for i in range(nconcurrent)]

    n_total = 0
    for f in ff:
        n_total += (await f)

    if eos_passthrough:
        await q_out.put(eos_marker)

    return n_total


async def aq2sq_pump(src, dst,
                     eos_marker=EOS_MARKER,
                     eos_passthrough=True,
                     dt=0.01):
    """ Pump from async Queue to synchronous queue.

        dt -- how much to sleep when dst is full
    """

    def safe_put(x, dst):
        try:
            dst.put_nowait(x)
        except queue.Full:
            return False

        return True

    async def push_to_dst(x, dst, dt):
        while not safe_put(x, dst):
            await asyncio.sleep(dt)

    while True:
        x = await src.get()

        if x is eos_marker:
            if eos_passthrough:
                await push_to_dst(x, dst, dt)

            src.task_done()
            break

        await push_to_dst(x, dst, dt)
        src.task_done()


async def q2q_nmap(func,
                   q_in,
                   q_out,
                   nconcurrent,
                   eos_marker=EOS_MARKER,
                   eos_passthrough=True,
                   dt=0.01,
                   loop=None):
    """Pump data from synchronous queue to another synchronous queue via a worker
       pool of async `func`s. Allow upto `nconcurrent` concurrent `func` tasks
       at a time.

                / [func] \
         q_in ->  [func]  >--> q_out
                \ [func] /


        - Order is not preserved.
        - func is expected not to raise exceptions
    """
    def safe_get(src):
        try:
            x = src.get_nowait()
            return (x, True)
        except queue.Empty:
            return (None, False)

    def safe_put(x, dst):
        try:
            dst.put_nowait(x)
        except queue.Full:
            return False

        return True

    async def push_to_dst(x, dst, dt):
        while not safe_put(x, dst):
            await asyncio.sleep(dt)

    async def intake_loop(src, dst, dt):
        while True:
            x, ok = safe_get(src)
            if not ok:
                await asyncio.sleep(dt)
            elif x is eos_marker:
                src.task_done()
                break
            else:
                await dst.put(x)
                src.task_done()

        for _ in range(nconcurrent):
            await dst.put(eos_marker)

        await dst.join()

    async def output_loop(src, dst, dt):
        while True:
            x = await src.get()

            if x is eos_marker:
                src.task_done()
                break

            await push_to_dst(x, dst, dt)
            src.task_done()

    aq_in = asyncio.Queue(nconcurrent*2)
    aq_out = asyncio.Queue(aq_in.maxsize)

    #                 / [func] \
    # q_in -> aq_in ->  [func]  >--> aq_out -> q_out
    #                 \ [func] /

    # Launch async worker pool: aq_in ->[func]-> aq_out
    for _ in range(nconcurrent):
        asyncio.ensure_future(async_q2q_map(func, aq_in, aq_out,
                                            eos_marker=eos_marker,
                                            eos_passthrough=False),
                              loop=loop)

    # Pump from aq_out -> q_out (async to sync interface)
    asyncio.ensure_future(output_loop(aq_out, q_out, dt), loop=loop)

    # Pump from q_in -> aq_in (sync to async interface)
    await intake_loop(q_in, aq_in, dt)

    # by this time all input items have been mapped through func and are in aq_out

    # terminate output pump
    await aq_out.put(eos_marker)  # tell output_loop to stop
    await aq_out.join()           # wait for ack, all valid data is in `q_out` now

    # finally push through eos_marker unless asked not too
    if eos_passthrough:
        await push_to_dst(eos_marker, q_out, dt)


class AsyncWorkerPool(object):
    """NxM worker pool, maintains N threads running M concurrent async tasks
    each. Provides synchronous access via iterators.
    """

    def __init__(self,
                 nthreads=1,
                 tasks_per_thread=1,
                 max_buffer=None):
        if max_buffer is None:
            max_buffer = min(100, nthreads*tasks_per_thread*2)

        self._pool = ThreadPoolExecutor(nthreads)
        self._nthreads = nthreads
        self._default_tasks_per_thread = tasks_per_thread
        self._q1 = queue.Queue(max_buffer)
        self._q2 = queue.Queue(max_buffer)
        self._tls = threading.local()
        self._map_state = None

        def bootstrap(tls):
            tls.loop = asyncio.new_event_loop()

        pool_broadcast(self._pool, bootstrap, self._tls)

    @staticmethod
    def _run_worker_thread(func, src, dst, tls, nconcurrent, state):
        loop = tls.loop

        q2q_task = q2q_nmap(func, src, dst,
                            nconcurrent,
                            eos_passthrough=False,
                            loop=loop)

        loop.run_until_complete(q2q_task)

        with state.lock:
            assert state.count > 0
            state.count -= 1

            if state.count == 0:
                dst.put(EOS_MARKER)

    def broadcast(self, func, *args, **kwargs):
        def action():
            loop = self._tls.loop
            xx = loop.run_until_complete(func(*args, **kwargs))
            return xx

        return pool_broadcast(self._pool, action)

    def submit(self, func, *args, **kwargs):
        def action():
            loop = self._tls.loop
            return loop.run_until_complete(func(*args, **kwargs))

        return self._pool.submit(action)

    def run_one(self, func, *args, **kwargs):
        return self.submit(func, *args, **kwargs).result()

    def running(self):
        return self._map_state is not None

    def unravel(self):
        _state = self._map_state
        if _state:
            _state.unravel()

    def map(self, func, its, nconcurrent=None, **kwargs):
        from time import sleep
        from threading import Lock

        if self._map_state is not None:
            raise RuntimeError("map is not re-entrant")

        dt = 0.01

        proc = func if len(kwargs) == 0 else (lambda x: func(x, **kwargs))

        if nconcurrent is None:
            nconcurrent = self._default_tasks_per_thread

        shared_state = SimpleNamespace(lock=Lock(),
                                       count=self._nthreads)
        feedq = self._q1
        outq = self._q2

        workers = [self._pool.submit(self._run_worker_thread,
                                     proc,
                                     feedq, outq,
                                     tls=self._tls,
                                     nconcurrent=nconcurrent,
                                     state=shared_state)
                   for _ in range(self._nthreads)]

        def unravel():
            # flush input queue
            while True:
                try:
                    feedq.get_nowait()
                except queue.Empty:
                    break

            # send EOS
            for _ in range(self._nthreads):
                feedq.put(EOS_MARKER)

            # flush output queue
            while True:
                x = outq.get()
                if x is EOS_MARKER:
                    break

            self._map_state = None

        self._map_state = SimpleNamespace(unravel=unravel)
        # max_tasks_at_once = len(workers)*nconcurrent

        # append EOS_MARKER for every worker thread
        its = itertools.chain(its, [EOS_MARKER]*len(workers))
        nin = 0
        nout = 0

        out_flushed = False

        def _out(x):
            nonlocal out_flushed
            nonlocal nout

            if x is EOS_MARKER:
                out_flushed = True
            else:
                nout += 1
                yield x

        for x in its:
            assert out_flushed is False

            while feedq.full():
                n = 0
                while feedq.full() and not out_flushed and not outq.empty():
                    n += 1
                    yield from _out(outq.get_nowait())

                if feedq.full() and n == 0:
                    sleep(dt)  # Avoid busy loop, feedq is full, but outq was empty

            feedq.put_nowait(x)
            nin += 1

            # flush upto 10 results for every input, so that we can catch up,
            # but at the same time don't neglect input side either. We can
            # probably be smarter and monitor tasks in flight using nin/nout
            # and decide based on that.
            for _ in range(10):
                if outq.empty():
                    break

                yield from _out(outq.get_nowait())

        # We are done with input, need to flush output queue now.
        while not out_flushed:
            yield from _out(outq.get())

        self._map_state = None

################################################################################
# tests below
################################################################################


def test_q2q_map():
    async def proc(x):
        await asyncio.sleep(0.01)
        return (x, x)

    loop = asyncio.new_event_loop()

    def run(**kwargs):
        q1 = asyncio.Queue(10)
        q2 = asyncio.Queue(10)

        for i in range(4):
            q1.put_nowait(i)
        q1.put_nowait(EOS_MARKER)

        async def run_test(**kwargs):
            await async_q2q_map(proc, q1, q2, **kwargs)
            await q1.join()

            xx = []
            while not q2.empty():
                xx.append(q2.get_nowait())
            return xx

        return loop.run_until_complete(run_test(**kwargs))

    expect = [(i, i) for i in range(4)]
    assert run() == expect + [EOS_MARKER]
    assert run(eos_passthrough=False) == expect

    loop.close()


def test_q2qnmap():
    import random

    async def proc(x, state, delay=0.1):
        state.active += 1

        delay = random.uniform(0, delay)
        await asyncio.sleep(delay)

        state.max_active = max(state.active, state.max_active)
        state.active -= 1
        return (x, x)

    def run_producer(n, q, eos_marker):
        for i in range(n):
            q.put(i)
        q.put(eos_marker)
        q.join()

    def run_consumer(q, eos_marker):
        xx = []
        while True:
            x = q.get()
            q.task_done()
            xx.append(x)
            if x is eos_marker:
                break

        return xx

    wk_pool = ThreadPoolExecutor(max_workers=2)
    src = queue.Queue(3)
    dst = queue.Queue(3)

    # first do self test of consumer/producer
    N = 100

    wk_pool.submit(run_producer, N, src, EOS_MARKER)
    xx = wk_pool.submit(run_consumer, src, EOS_MARKER)
    xx = xx.result()

    assert len(xx) == N + 1
    assert len(set(xx) - set(range(N)) - set([EOS_MARKER])) == 0
    assert src.qsize() == 0

    loop = asyncio.new_event_loop()

    def run(N, nconcurrent, delay, eos_passthrough=True):
        async def run_test(func, N, nconcurrent):
            wk_pool.submit(run_producer, N, src, EOS_MARKER)
            xx = wk_pool.submit(run_consumer, dst, EOS_MARKER)
            await q2q_nmap(func, src, dst, nconcurrent, eos_passthrough=eos_passthrough)

            if eos_passthrough is False:
                dst.put(EOS_MARKER)

            return xx.result()

        state = SimpleNamespace(active=0, max_active=0)
        func = lambda x: proc(x, delay=delay, state=state)
        return state, loop.run_until_complete(run_test(func, N, nconcurrent))

    expect = set([(x, x) for x in range(N)] + [EOS_MARKER])

    st, xx = run(N, 20, 0.1)
    assert len(xx) == N + 1
    assert 1 < st.max_active <= 20
    assert set(xx) == expect

    st, xx = run(N, 4, 0.01)
    assert len(xx) == N + 1
    assert 1 < st.max_active <= 4
    assert set(xx) == expect

    st, xx = run(N, 4, 0.01, eos_passthrough=False)
    assert len(xx) == N + 1
    assert 1 < st.max_active <= 4
    assert set(xx) == expect


def test_async_work_pool():
    async def proc(x, delay):
        await asyncio.sleep(delay)
        return (x, x)

    proc2 = lambda x: proc(x, delay=0.01)

    expect = set((x, x) for x in range(100))

    pool = AsyncWorkerPool(2, 10, max_buffer=30)

    xx = list(pool.map(proc, range(100), delay=0.1))

    assert len(xx) == 100
    assert expect == set(xx)

    xx = list(pool.map(proc2, range(100)))
    assert len(xx) == 100
    assert expect == set(xx)


def test_gen2q():

    async def gen_func(idx, state):
        if state.count >= state.max_count:
            return EOS_MARKER

        cc = state.count
        state.count += 1

        await asyncio.sleep(state.dt)
        return cc

    async def sink(q):
        xx = []
        while True:
            x = await q.get()
            if x is EOS_MARKER:
                return xx
            xx.append(x)
        return xx

    async def run_async(nconcurrent, max_count=100, dt=0.1):
        state = SimpleNamespace(count=0,
                                max_count=max_count,
                                dt=dt)
        gen = lambda idx: gen_func(idx, state)

        q = asyncio.Queue(maxsize=10)
        g2q = asyncio.ensure_future(gen2q_async(gen, q, nconcurrent))
        xx = await sink(q)
        return g2q.result(), xx

    loop = asyncio.new_event_loop()

    def run(*args, **kwargs):
        return loop.run_until_complete(run_async(*args, **kwargs))

    n, xx = run(10, max_count=100, dt=0.1)
    assert len(xx) == n
    assert len(xx) == 100
    assert set(xx) == set(range(100))
