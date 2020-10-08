import json
import pathlib
from datetime import datetime, timedelta
from types import SimpleNamespace
from uuid import UUID

from odc.index.stac import stac_transform
from odc.stats._gm import gm_product
from odc.stats.model import DateTimeRange, Task
from odc.stats.tasks import TaskReader
from odc.stats.utils import (CompressedDataset, bin_annual, bin_full_history,
                             bin_generic, bin_seasonal, mk_season_rules,
                             season_binner)

TEST_DIR = pathlib.Path(__file__).parent.absolute()


def gen_compressed_dss(n,
                       dt0=datetime(2010, 1, 1, 11, 30, 27),
                       step=timedelta(days=1)):
    if isinstance(step, int):
        step = timedelta(days=step)

    dt = dt0
    for i in range(n):
        yield CompressedDataset(UUID(int=i), dt)
        dt = dt + step


def test_stac():
    product = gm_product(location='/tmp/')
    reader = TaskReader(str(TEST_DIR / 'test_tiles.db'), product)
    task = reader.load_task(reader.all_tiles[0])

    stac_meta = task.render_metadata()
    odc_meta = stac_transform(stac_meta)

    # with open(TEST_DIR / 'meta.json', 'w') as outfile:
    #     json.dump(task.render_metadata(), outfile, indent=2)

    # with open(TEST_DIR / 'odc-meta.json', 'w') as outfile:
    #     json.dump(odc_meta, outfile, indent=2)


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

    tasks = bin_seasonal(cells, 6, 1)
    verify(tasks)


def test_season_binner():
    four_seasons_rules = {
        12: '12--P3M', 1: '12--P3M', 2: '12--P3M',
        3: '03--P3M', 4: '03--P3M', 5: '03--P3M',
        6: '06--P3M', 7: '06--P3M', 8: '06--P3M',
        9: '09--P3M', 10: '09--P3M', 11: '09--P3M',
    }

    assert mk_season_rules(3, anchor=12) == four_seasons_rules

    binner = season_binner(four_seasons_rules)
    assert binner(datetime(2020, 1, 28)) == '2019-12--P3M'
    assert binner(datetime(2020, 2, 21)) == '2019-12--P3M'
    assert binner(datetime(2020, 3, 1)) == '2020-03--P3M'
    assert binner(datetime(2020, 4, 1)) == '2020-03--P3M'
    assert binner(datetime(2020, 5, 31)) == '2020-03--P3M'
    assert binner(datetime(2020, 6, 1)) == '2020-06--P3M'
    assert binner(datetime(2020, 12, 30)) == '2020-12--P3M'

    binner = season_binner({})
    for m in range(1, 13):
        assert binner(datetime(2003, m, 10)) == ''

    binner = season_binner({1: '01--P1M'})
    assert binner(datetime(2001, 10, 19)) == ''
    assert binner(datetime(2001, 1, 19)) == '2001-01--P1M'
