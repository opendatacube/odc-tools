import numpy as np
import xarray as xr
import dask.array as da
from odc.stats._fc_percentiles import StatsFCP
import pytest 
import pandas as pd


@pytest.fixture
def dataset():
    band_1 = np.array([
        [[255, 57], [20, 50]],
        [[30, 40], [70, 80]], 
        [[25, 52], [73, 98]], 
    ]).astype(np.uint8)

    band_2 = np.array([
        [[0, 128], [0, 0]],
        [[0, 0], [0, 0]], 
        [[0, 0], [0, 0]], 
    ]).astype(np.uint8)

    band_1 = da.from_array(band_1, chunks=(3, -1, -1))
    band_2 = da.from_array(band_2, chunks=(3, -1, 20))

    tuples = [(np.datetime64(f"2000-01-01T0{i}"), np.datetime64(f"2000-01-01")) for i in range(3)]
    index = pd.MultiIndex.from_tuples(tuples, names=["time", "solar_day"])
    coords = {
        "x": np.linspace(10, 20, band_1.shape[2]), 
        "y": np.linspace(0, 5, band_1.shape[1]), 
        "spec": index,
    }

    data_vars = {"band_1": (("spec", "y", "x"), band_1), "water": (("spec", "y", "x"), band_2)}
    xx = xr.Dataset(data_vars=data_vars, coords=coords)
    return xx


@pytest.mark.parametrize("bits", [0b0000_0000, 0b0001_0000])
def test_native_transform(dataset, bits):
    
    xx = dataset.copy()
    xx['water'] = da.bitwise_or(xx['water'], bits)
    xx = StatsFCP._native_tr(xx)
    
    expected_result = np.array([
        [[255, 57], [20, 50]],
        [[30, 40], [70, 80]], 
        [[25, 52], [73, 98]],
    ])
    result = xx.compute()["band_1"].data
    assert (result == expected_result).all()

    expected_result = np.array([
        [[False, True], [False, False]],
        [[False, False], [False, False]], 
        [[False, False], [False, False]],
    ])
    result = xx.compute()["wet"].data
    assert (result == expected_result).all()

    expected_result = np.array([
        [[True, False], [True, True]],
        [[True, True], [True, True]], 
        [[True, True], [True, True]],
    ])
    result = xx.compute()["dry"].data
    assert (result == expected_result).all()


def test_fusing(dataset):
    xx = StatsFCP._native_tr(dataset)
    xx = xx.groupby("solar_day").map(StatsFCP._fuser)

    expected_result = np.array(
        [[30, 57], [20, 50]],
    )
    result = xx.compute()["band_1"].data
    assert (result == expected_result).all()

    expected_result = np.array(
        [[False, True], [False, False]],
    )
    result = xx.compute()["wet"].data
    assert (result == expected_result).all()

    expected_result = np.array(
        [[True, False], [True, True]],
    )
    result = xx.compute()["dry"].data
    assert (result == expected_result).all()


def test_reduce(dataset):
    fcp = StatsFCP()
    xx = fcp._native_tr(dataset)
    xx = fcp.reduce(xx)

    result = xx.compute()["band_1_pc_10"].data
    assert (result[0, :] == 255).all()
    assert (result[1, :] != 255).all()

    expected_result = np.array(
        [[1, 0], [2, 2]],
    )
    result = xx.compute()["qa"].data
    assert (result == expected_result).all()
    
    assert set(xx.data_vars.keys()) == set(
        ["band_1_pc_10", "band_1_pc_50", "band_1_pc_90", "qa"]
    )

    for band_name in xx.data_vars.keys():
        assert xx.data_vars[band_name].dtype == np.uint8
    