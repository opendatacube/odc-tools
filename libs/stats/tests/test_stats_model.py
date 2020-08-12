from odc.stats.model import DateTimeRange
from datetime import datetime, timedelta


def test_dt_range():
    _1us = timedelta(microseconds=1)

    dtr = DateTimeRange('2019', '1Y')
    print(dtr)
    assert dtr.freq == '1Y'
    assert dtr.start == datetime(2019, 1, 1)
    assert dtr.end == datetime(2020, 1, 1) - _1us
    assert dtr.short == '2019--P1Y'
    assert DateTimeRange(dtr.short) == dtr
    st = dtr.to_stac()
    assert st['datetime'] == st['dtr:start_datetime']
    assert st['dtr:start_datetime'] == '2019-01-01T00:00:00.000000Z'
    assert st['dtr:end_datetime'] == '2019-12-31T23:59:59.999999Z'

    dtr = DateTimeRange('2019-03-07--P1M')
    assert dtr.short == '2019-03-07--P1M'
    assert dtr.freq == '1M'
    assert dtr.start == datetime(2019, 3, 7)
    assert dtr.end == datetime(2019, 4, 7) - _1us

    assert str(dtr) == dtr.short
    assert repr(dtr).startswith('DateTimeRange')
