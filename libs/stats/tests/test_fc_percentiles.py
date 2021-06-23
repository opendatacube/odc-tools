import numpy as np
import xarray as xr
import dask.array as da
from odc.stats._fc_percentiles import StatsFCP
from odc.algo._masking import _nodata_fuser
import pytest 
import pandas as pd


@pytest.fixture
def dataset():
    band_1 = np.array([
        [[255, 57], [20, 50]],
        [[30, 40], [70, 80]], 
    ])

    band_2 = np.array([
        [[0, 128], [0, 0]],
        [[0, 0], [0, 0]], 
    ])

    tuples = [(np.datetime64(f"2000-01-01T0{i}"), np.datetime64(f"2000-01-01")) for i in range(2)]
    index = pd.MultiIndex.from_tuples(tuples, names=["time", "solar_day"])
    coords = {
        "x": np.linspace(10, 20, band_1.shape[2]), 
        "y": np.linspace(0, 5, band_1.shape[1]), 
        "spec": index,
    }

    data_vars = {"band_1": (("spec", "y", "x"), band_1), "water": (("spec", "y", "x"), band_2)}
    xx = xr.Dataset(data_vars=data_vars, coords=coords)
    return xx


def test_native_transform(dataset):
    
    xx = StatsFCP._native_tr(dataset())
    expected_result = np.array([
        [[255, 255], [20, 50]],
        [[30, 40], [70, 80]], 
    ])

    assert (xx.compute() == expected_result).all()


def test_native_transform(dataset):
    
    xx = StatsFCP._native_tr(dataset)
    
    expected_result = np.array([
        [[255, 255], [20, 50]],
        [[30, 40], [70, 80]], 
    ])

    result = xx.compute()["band_1"].data

    assert (result == expected_result).all()


def test_fusing(dataset):
    
    xx = StatsFCP._native_tr(dataset)
    xx = xx.groupby("solar_day").map(_nodata_fuser)
    print(xx)
    assert True