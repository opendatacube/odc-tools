import pytest
from odc.stats.proc import TaskRunner, TaskRunnerConfig, get_cpu_quota, get_mem_quota

def test_quotas():
    cpuq = get_cpu_quota()
    memq = get_mem_quota()
    assert cpuq is None or isinstance(cpuq, float)
    assert memq is None or isinstance(memq, int)

def test_runner_product_cfg(test_db_path, dummy_plugin_name):
    cfg = TaskRunnerConfig(
        filedb=test_db_path,
        plugin=dummy_plugin_name,
        product={"name": "ga_test", "short_name": "tt", "version": "3.2.1"},
        output_location="s3://data-bucket/{product}/{version}",
        plugin_config={"bands": ["red", "nir"]},
        cog_opts={
            "compression": "deflate",
            "zlevel": 9,
            "blocksize": 1024,
            "overrides": {"rgba": {"compression": "webp", "webp_level": 80}},
        },
    )
    runner = TaskRunner(cfg)
    assert runner.product.name == "ga_test"
    assert runner.product.short_name == "tt"
    assert runner.product.version == "3.2.1"
    assert runner.product.location == "s3://data-bucket/ga_test/3-2-1"
    assert runner.product.measurements == ("red", "nir")

    cfg = runner.sink.cog_opts("red")
    assert "overrides" not in cfg
    assert cfg["blocksize"] == 1024
    assert cfg["compression"] == "deflate"
    assert cfg["zlevel"] == 9

    cfg = runner.sink.cog_opts("rgba")
    assert "overrides" not in cfg
    assert cfg["blocksize"] == 1024
    assert cfg["compression"] == "webp"
    assert cfg["webp_level"] == 80


def test_plugin_resolve():
    from odc.stats.plugins import resolve
    from odc.stats.plugins.gm import StatsGM

    assert resolve("gm-generic") is not None
    assert resolve("odc.stats.plugins.gm.StatsGM") is not None

    # Test no such class
    with pytest.raises(ValueError):
        resolve("odc.nosuch.Class")

    # Test wrong class
    with pytest.raises(ValueError):
        resolve("queue.Queue")
