from types import SimpleNamespace
import timeit


def _rr2s(r):
    return '''
Count: {r.count:,d}
       {fps:.1f} per second
Total: {r.total:6.3f} sec
TTFB : {r.ttfb:6.3f} sec
.....: {r.uu:032X}
..
'''.format(r=r, fps=r.count/r.total).strip()


def ds_stream_test_func(dss, get_uuid=None):
    def default_get_uuid(ds):
        return ds.id

    if get_uuid is None:
        get_uuid = default_get_uuid

    timer = timeit.default_timer
    uu = 0
    count = 0
    t00 = timer()
    t0 = None
    for ds in dss:
        t0 = t0 or timer()
        uu ^= get_uuid(ds).int
        count += 1

    t_end = timer()

    rr = SimpleNamespace(uu=uu,
                         count=count,
                         ttfb=t0-t00,
                         total=t_end-t00,
                         text='')
    rr.text = _rr2s(rr)
    return rr
