from click.testing import CliRunner

from odc.apps.dc_tools.esri_land_cover_to_dc import cli


# Test the actual process
def test_indexing_cli():
    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "--add-product",
            "--limit",
            "10"
        ],
    )
    assert result.exit_code == 0
