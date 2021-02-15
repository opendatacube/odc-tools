import pathlib
import pytest

TEST_DIR = pathlib.Path(__file__).parent.absolute()


@pytest.fixture
def test_dir():
    return TEST_DIR


@pytest.fixture
def test_db(test_dir):
    return test_dir / "test_tiles.db"
