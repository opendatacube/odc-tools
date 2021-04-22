import pytest
from mock import patch
from odc.stats._gm_ml_pred import PredGMS2


@pytest.fixture
def dummy_pred_gms2():
    return PredGMS2()


@pytest.mark.skip(reason="integration check the input_data methods only")
def test_pred_gms2_pred_input(dummy_pred_gms2):
    from dask.distributed import Client
    client = Client(processes=False)
    with patch('odc.stats.model.Task') as mock:
        instance = mock.return_value
        instance.tile_index = (220, 77)
        ds = dummy_pred_gms2.input_data(instance)
        print(ds)
        assert ds
