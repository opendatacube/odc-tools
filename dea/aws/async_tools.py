import asyncio
import itertools
import logging

EOS_MARKER = object()
log = logging.getLogger(__name__)


async def process_stream(stream, nworkers, async_proc, loop=None):
    """This is equivalent to a synchronous `map`, but with limited concurrency, stream
    will run upto nworkers concurrently.

    ```
      for x in stream:
         async_proc(x)
    ```
    """
    async def process_q(q, async_proc):
        while True:
            x = await q.get()
            if x is EOS_MARKER:
                q.task_done()
                return

            try:
                await async_proc(x)
            except Exception as e:
                log.error("Uncaught exception: %s", str(e))

            q.task_done()

    async def feed_q(stream, q, n_consumers):
        stream = itertools.chain(stream, itertools.repeat(EOS_MARKER, n_consumers))
        for x in stream:
            await q.put(x)
        await q.join()

    q = asyncio.Queue(nworkers*2, loop=loop)
    for _ in range(nworkers):
        asyncio.ensure_future(process_q(q, async_proc), loop=loop)

    return await feed_q(stream, q, nworkers)


def async_process(stream,
                  nworkers,
                  async_proc,
                  loop=None):

    if loop is None:
        loop = asyncio.get_event_loop()

    loop.run_until_complete(process_stream(stream, nworkers, async_proc, loop=loop))


def p_fetch(stream, on_data, nconnections=64, loop=None):
    """
      stream -- ((userdata, req)....)


   ```
      for udata, req in stream:
          status, headers, data = do_http_get(req)
          on_data(data, userdata, status=status, headers=headers, time=(t0,t1,t2))
    ```

    t0 - request is made
    t1 - response header is received and parsed
    t2 - response data is read
    """
    import aiohttp
    from timeit import default_timer as t_now

    async def fetch_one(req, session, userdata):
        t0 = t_now()
        try:
            async with session.get(req.full_url, headers=req.headers) as response:
                t1 = t_now()
                try:
                    data = await response.read()
                    t2 = t_now()

                    on_data(data, userdata,
                            status=response.status,
                            headers=response.headers,
                            time=(t0, t1, t2))

                except Exception as e:
                    log.error("Failed response.read: %s", str(e))
        except Exception as e:
            log.error("Failed to make request: %s", str(e))

    async def fetch_all(reqs, on_data, nconnections, loop=loop):
        tcp_connector = aiohttp.TCPConnector(limit=nconnections,
                                             loop=loop,
                                             limit_per_host=nconnections)

        async with aiohttp.ClientSession(loop=loop, connector=tcp_connector) as session:
            return await process_stream(reqs, nconnections, lambda x: fetch_one(x[1], session, x[0]), loop=loop)

    is_loop_mine = loop is None

    if loop is None:
        loop = asyncio.new_event_loop()

    loop.run_until_complete(fetch_all(stream, on_data, nconnections, loop=loop))

    if is_loop_mine:
        loop.close()
