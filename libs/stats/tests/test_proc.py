from odc.stats.proc import TaskRunner, TaskRunnerConfig


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
