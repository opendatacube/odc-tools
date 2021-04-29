import numpy as np
from odc.algo._warp import _shrink2


def test_shrink2_smoke_test():
    assert _shrink2(np.zeros((15, 17, 3), dtype='uint8')).shape == (8, 9, 3)
    assert _shrink2(np.zeros((15, 17), dtype='uint8')).shape == (8, 9)
    assert _shrink2(np.zeros((2, 15, 17), dtype='uint8'), axis=1).shape == (2, 8, 9)
