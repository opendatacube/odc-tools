import numpy as np
import xarray as xr
import dask.array as da
from odc.stats._pq_bitmask import StatsPQLSBitmask
import pytest
import pandas as pd


@pytest.fixture
def dataset():
    band_red = np.array([
        [[255, 57], [20, 50]],
        [[30, 0], [70, 80]],
        [[25, 52], [0, 0]],
    ])
    cloud_mask = 0b0000_0000_0001_1010
    no_data = 0b0000_0000_0000_0001
    band_pq = np.array([
        [[0, 0], [cloud_mask, no_data]],
        [[no_data, 0], [0, 0]],
        [[0, cloud_mask], [cloud_mask, 0]],
    ])

    band_red = da.from_array(band_red, chunks=(3, -1, -1))
    band_pq = da.from_array(band_pq, chunks=(3, -1, -1))

    tuples = [(np.datetime64(f"2000-01-01T0{i}"), np.datetime64(f"2000-01-01")) for i in range(3)]
    index = pd.MultiIndex.from_tuples(tuples, names=["time", "solar_day"])
    coords = {
        "x": np.linspace(10, 20, band_red.shape[2]),
        "y": np.linspace(0, 5, band_pq.shape[1]),
        "spec": index,
    }

    data_vars = {"band_red": (("spec", "y", "x"), band_red), "QA_PIXEL": (("spec", "y", "x"), band_pq)}
    attrs = dict(crs="epsg:32633", grid_mapping="spatial_ref")
    xx = xr.Dataset(data_vars=data_vars, coords=coords, attrs=attrs)
    xx['band_red'].attrs['nodata'] = 0
    return xx


def test_native_transform(dataset):
    pq = StatsPQLSBitmask()

    xx = pq._native_tr(dataset)
    tr_result = xx.compute()

    expected_result = np.array([
        [[True, True], [True, False]],
        [[False, True], [True, True]],
        [[True, True], [True, True]],
    ])
    keeps = tr_result["keeps"].data
    assert (keeps == expected_result).all()

    expected_result = np.array([
        [[True, True], [False, True]],
        [[True, True], [True, True]],
        [[True, False], [False, True]],
    ])
    clear = tr_result["clear"].data
    assert (clear == expected_result).all()


def test_fuser(dataset):
    pq = StatsPQLSBitmask()

    xx = pq._native_tr(dataset)
    xx = xx.groupby("solar_day").map(pq._fuser)
    fuser_result = xx.compute()

    expected_result = np.array(
        [[True, True], [True, True]],
    )
    result = fuser_result["clear"].data
    assert (result == expected_result).all()

def test_reduce(dataset):
    pq = StatsPQLSBitmask(filters=[[1,1,0]])

    xx = pq._native_tr(dataset)
    xx = pq.reduce(xx)
    reduce_result = xx.compute()
    print(reduce_result)

    assert set(reduce_result.data_vars.keys()) == set(
        ["total", "clear", "clear_1_1_0"]
    )

    expected_result = np.array(
        [[2, 3], [3, 2]]
    )
    total = reduce_result["total"].data
    assert (total == expected_result).all()

    expected_result = np.array(
        [[2, 2], [1, 2]]
    )
    clear = reduce_result["clear"].data
    assert (clear == expected_result).all()

    expected_result = np.array(
        [[1, 2], [2, 1]]
    )
    clear_1_1_0 = reduce_result["clear_1_1_0"].data
    assert (clear_1_1_0 == expected_result).all()