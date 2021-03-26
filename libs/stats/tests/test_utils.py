from datetime import datetime, timedelta
from types import SimpleNamespace

import pystac
import pytest
from odc.index.stac import stac_transform
from odc.stats.model import DateTimeRange
from odc.stats.tasks import TaskReader
from odc.stats.utils import (
    bin_annual,
    bin_full_history,
    bin_generic,
    bin_seasonal,
    mk_season_rules,
    season_binner,
)

from . import gen_compressed_dss


def test_stac(test_db_path):
    from odc.stats._gm import StatsGMS2

    product = StatsGMS2().product(location="/tmp/")
    reader = TaskReader(test_db_path, product)
    task = reader.load_task(reader.all_tiles[0])

    stac_meta = task.render_metadata()
    odc_meta = stac_transform(stac_meta)

    # TODO: actually test content of odc_meta?
    assert isinstance(odc_meta, dict)

    stac_item = pystac.Item.from_dict(stac_meta)
    stac_item.validate()


def test_binning():
    dss = list(gen_compressed_dss(100, dt0=datetime(2000, 1, 1), step=27))
    cells = {
        (0, 1): SimpleNamespace(
            dss=dss, geobox=None, idx=None, utc_offset=timedelta(seconds=0)
        )
    }

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
        12: "12--P3M",
        1: "12--P3M",
        2: "12--P3M",
        3: "03--P3M",
        4: "03--P3M",
        5: "03--P3M",
        6: "06--P3M",
        7: "06--P3M",
        8: "06--P3M",
        9: "09--P3M",
        10: "09--P3M",
        11: "09--P3M",
    }

    assert mk_season_rules(3, anchor=12) == four_seasons_rules

    binner = season_binner(four_seasons_rules)
    assert binner(datetime(2020, 1, 28)) == "2019-12--P3M"
    assert binner(datetime(2020, 2, 21)) == "2019-12--P3M"
    assert binner(datetime(2020, 3, 1)) == "2020-03--P3M"
    assert binner(datetime(2020, 4, 1)) == "2020-03--P3M"
    assert binner(datetime(2020, 5, 31)) == "2020-03--P3M"
    assert binner(datetime(2020, 6, 1)) == "2020-06--P3M"
    assert binner(datetime(2020, 12, 30)) == "2020-12--P3M"

    binner = season_binner({})
    for m in range(1, 13):
        assert binner(datetime(2003, m, 10)) == ""

    binner = season_binner({1: "01--P1M"})
    assert binner(datetime(2001, 10, 19)) == ""
    assert binner(datetime(2001, 1, 19)) == "2001-01--P1M"


@pytest.mark.parametrize(
    "months,anchor",
    [(6, 1)]
)
def test_bin_seasonal_mk_season_rules(months, anchor):
    season_rules = mk_season_rules(months, anchor)
    assert {'01--P6M', '07--P6M'} == set(season_rules.values())


