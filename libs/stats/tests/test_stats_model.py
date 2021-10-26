from datetime import datetime, timedelta
import pytest

from odc.stats._cli_common import parse_all_tasks
from odc.stats._cli_common import parse_task as cli_parse_task
from odc.stats.model import DateTimeRange
from odc.stats.tasks import parse_task, render_task
from . import DummyPlugin


def test_dt_range():
    _1us = timedelta(microseconds=1)

    dtr = DateTimeRange("2019", "1Y")
    print(dtr)
    assert dtr.freq == "1Y"
    assert dtr.start == datetime(2019, 1, 1)
    assert dtr.end == datetime(2020, 1, 1) - _1us
    assert dtr.short == "2019--P1Y"
    assert DateTimeRange(dtr.short) == dtr

    dtr = DateTimeRange("2019-03-07--P1M")
    assert dtr.short == "2019-03-07--P1M"
    assert dtr.freq == "1M"
    assert dtr.start == datetime(2019, 3, 7)
    assert dtr.end == datetime(2019, 4, 7) - _1us

    assert str(dtr) == dtr.short
    assert repr(dtr).startswith("DateTimeRange")

    assert DateTimeRange.year(2019) == DateTimeRange("2019--P1Y")

    qq = DateTimeRange.year(2017).dc_query()
    assert qq == {"time": (datetime(2017, 1, 1), datetime(2018, 1, 1) - _1us)}

    qq = DateTimeRange.year(2017).dc_query(pad=1)
    assert qq == {"time": (datetime(2016, 12, 31), datetime(2018, 1, 2) - _1us)}

    assert datetime(2017, 1, 1) in DateTimeRange("2017--P1Y")
    assert datetime(2017, 6, 29) in DateTimeRange("2017--P1Y")
    assert datetime(2018, 6, 29) not in DateTimeRange("2017--P1Y")
    assert datetime(2018, 1, 1) not in DateTimeRange("2017--P1Y")

    assert DateTimeRange.year(2000) + 1 == DateTimeRange.year(2001)
    assert DateTimeRange.year(2000) - 1 == DateTimeRange.year(1999)


def test_parse_task():
    assert parse_task("2017--P1Y/3/4") == ("2017--P1Y", 3, 4)
    assert parse_task("2017--P1Y,+3,+004") == ("2017--P1Y", 3, 4)
    assert parse_task("2017--P1Y,x+3,y+004") == ("2017--P1Y", 3, 4)
    assert parse_task("x+003/y-004/2018--P3Y") == ("2018--P3Y", 3, -4)


@pytest.mark.parametrize(
    "tidx",
    [
        ("2017--P1Y", +3, +4),
        ("2018--P1Y", -3, +4),
        ("2019--P1Y", -3, -4),
        ("2013--P1Y", +3, -4),
    ],
)
def test_parse_render_task(tidx):
    assert parse_task(render_task(tidx)) == tidx
    assert cli_parse_task(render_task(tidx)) == tidx


def test_parse_all_tasks():
    all_tasks = [("2019--P1Y", 1, i) for i in range(10)]

    assert parse_all_tasks(["::2"], all_tasks) == all_tasks[::2]
    assert parse_all_tasks(["0", "1", str(len(all_tasks) - 1)], all_tasks) == [
        all_tasks[i] for i in [0, 1, len(all_tasks) - 1]
    ]
    assert parse_all_tasks(["0", "2019--P1Y/1/1"], all_tasks) == [
        all_tasks[i] for i in [0, 1]
    ]
    assert parse_all_tasks(["0", "2019--P1Y,+001,+001"], all_tasks) == [
        all_tasks[i] for i in [0, 1]
    ]

    for bad in (
        ["0", "1", "10"],  # 10 is out of range
        ["2000--P1Y/3/3"],  # no such task
        ["jjjk"],  # int doesn't parse
        ["klkl:opop"],  # slice int doesn't parse
    ):
        with pytest.raises(ValueError):
            parse_all_tasks(bad, all_tasks)


def test_product_for_plugin():
    from odc.stats.model import product_for_plugin
    plugin = DummyPlugin(bands=("red", "green"))
    product = product_for_plugin(plugin, location="file:///tmp/{product}/v{version_raw}")
    assert product.name == DummyPlugin.NAME
    assert product.short_name == DummyPlugin.SHORT_NAME
    assert product.version == DummyPlugin.VERSION
    assert product.location == f"file:///tmp/{product.name}/v{product.version}"
    assert product.measurements == ("red", "green")
    assert product.properties["odc:product_family"] == DummyPlugin.PRODUCT_FAMILY
    assert product.href == f"https://collections.dea.ga.gov.au/product/{product.name}"

    product = product_for_plugin(plugin,
        "file:///tmp/{product}/v{version}",
        name="custom-name",
        short_name="custom-short-name",
        version="custom-version",
        product_family="custom-family",
        producer="custom-producer",
        properties={"odc:custom-key": 33},
        collections_site="custom-site.com",
    )

    assert product.location == f"file:///tmp/{product.name}/v{product.version}"
    assert product.name == "custom-name"
    assert product.short_name == "custom-short-name"
    assert product.version == "custom-version"
    assert product.measurements == ("red", "green")
    assert product.properties["odc:product_family"] == "custom-family"
    assert product.properties["odc:producer"] == "custom-producer"
    assert product.properties["odc:custom-key"] == 33
    assert product.href == f"https://custom-site.com/product/{product.name}"

    assert product.region_code((1, 22)) == 'x01y22'
