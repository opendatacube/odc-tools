from odc.stats.proc import TaskRunner, TaskRunnerConfig


def test_runner_product_cfg(test_db_path, dummy_plugin_name):
    cfg = TaskRunnerConfig(
        filedb=test_db_path,
        plugin=dummy_plugin_name,
        product={"name": "ga_test", "short_name": "tt", "version": "3.2.1"},
        output_location="s3://data-bucket/{product}/v{version}",
        plugin_config={'bands': ['red', 'nir']}
    )
    runner = TaskRunner(cfg)
    assert runner.product.name == "ga_test"
    assert runner.product.short_name == "tt"
    assert runner.product.version == "3.2.1"
    assert runner.product.location == 's3://data-bucket/ga_test/v3.2.1'
    assert runner.product.measurements == ('red', 'nir')
