import pytest
from mock import patch
from odc.stats._gm_ml_pred import PredGMS2


@pytest.fixture
def dummy_pred_gms2():
    return PredGMS2()


def test_pred_gms2_pred_input(dummy_pred_gms2):
    with patch('odc.stats.model.Task') as mock:
        instance = mock.return_value
        instance.tile_index = (220, 77)
        ds = dummy_pred_gms2.input_data(instance)
        assert ds
