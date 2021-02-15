import pathlib
import pytest
from . import DummyPlugin

TEST_DIR = pathlib.Path(__file__).parent.absolute()


@pytest.fixture
def test_dir():
    return TEST_DIR


@pytest.fixture
def test_db_path(test_dir):
    return str(test_dir / "test_tiles.db")


@pytest.fixture
def dummy_plugin_name():
    from odc.stats._plugins import register

    name = "dummy-plugin"
    register(name, DummyPlugin)
    return name
