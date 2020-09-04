from uuid import UUID
from datetime import datetime, timedelta
from types import SimpleNamespace
from odc.stats.utils import bin_seasonal, bin_annual, bin_full_history, find_seasonal_bin, CompressedDataset, bin_generic
from odc.stats.model import DateTimeRange


def gen_compressed_dss(n,
                       dt0=datetime(2010, 1, 1, 11, 30, 27),
                       step=timedelta(days=1)):
    if isinstance(step, int):
        step = timedelta(days=step)

    dt = dt0
    for i in range(n):
        yield CompressedDataset(UUID(int=i), dt)
        dt = dt + step


def test_find_seasonal_bin():
    assert find_seasonal_bin(datetime(2020, 1, 3), 3, 3) == DateTimeRange('2019-12--P3M')
    assert find_seasonal_bin(datetime(2019, 12, 30), 3, 3) == DateTimeRange('2019-12--P3M')
    assert find_seasonal_bin(datetime(2019, 12, 1), 3, 3) == DateTimeRange('2019-12--P3M')
    assert find_seasonal_bin(datetime(2020, 2, 28), 3, 3) == DateTimeRange('2019-12--P3M')
    assert find_seasonal_bin(datetime(2020, 1, 3), 3, 3) == DateTimeRange('2020-03--P3M')


def test_binning():
    dss = list(gen_compressed_dss(100,
                                  dt0=datetime(2000, 1, 1),
                                  step=27))
    cells = {(0, 1): SimpleNamespace(dss=dss, geobox=None, idx=None)}

    def verify(task):
        for (t, *_), dss in task.items():
            period = DateTimeRange(t)
            assert all(ds.time in period for ds in dss)

    start = min(ds.time for ds in dss)
    end = max(ds.time for ds in dss)

    tasks_y = bin_annual(cells)
    verify(tasks_y)

    tasks_fh = bin_full_history(cells, start, end)
    verify(tasks_fh)

    yy_bins = [DateTimeRange(t) for t, *_ in tasks_y]

    tasks = bin_generic(cells, yy_bins)
    verify(tasks)
    for t in yy_bins:
        k = (t.short, 0, 1)
        dss1 = tasks[k]
        dss2 = tasks_y[k]

        assert set(ds.id for ds in dss1) == set(ds.id for ds in dss2)

    tasks = bin_seasonal(cells, start, end, 6, 1)
    verify(tasks)
