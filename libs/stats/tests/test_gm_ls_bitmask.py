import numpy as np
import xarray as xr
import dask.array as da
from odc.stats._gm_ls_bitmask import StatsGMLSBitmask
import pytest
import pandas as pd


@pytest.fixture
def dataset():
    band_red = np.array([
        [[255, 57], [20, 50]],
        [[30, 0], [70, 80]],
        [[25, 52], [0, 0]],
    ])
    cloud_mask = 0b0000_0000_0001_1000
    no_data = 0b0000_0000_0000_0001
    band_pq = np.array([
        [[0, 0], [0, no_data]],
        [[no_data, 0], [0, 0]],
        [[0, cloud_mask], [0, 0]],
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
    gm = StatsGMLSBitmask(["band_red"])

    xx = gm._native_tr(dataset)
    print(gm)
    expected_result = np.array([
        [[255, 57], [20, 0]],
        [[0, 0], [70, 80]],
        [[25, 52], [0, 0]],
    ])
    result = xx.compute()["band_red"].data
    assert (result == expected_result).all()

    expected_result = np.array([
        [[True, True], [True, True]],
        [[True, True], [True, True]],
        [[True, False], [True, True]],
    ])
    result = xx.compute()["cloud_mask"].data
    assert (result == expected_result).all()

def test_fuser(dataset):
    gm = StatsGMLSBitmask(["band_red"])

    xx = gm._native_tr(dataset)
    xx = xx.groupby("solar_day").map(gm._fuser)

    expected_result = np.array(
        [[255, 57], [20, 80]],
    )
    result = xx.compute()["band_red"].data
    assert (result == expected_result).all()

    expected_result = np.array(
        [[True, False], [True, True]],
    )
    result = xx.compute()["cloud_mask"].data
    assert (result == expected_result).all()

    # test fuser with filters
    gm = StatsGMLSBitmask(["band_red"], "QA_PIXEL", [0, 2])

    xx = gm._native_tr(dataset)
    xx = xx.groupby("solar_day").map(gm._fuser)

    expected_result = np.array(
        [[True, True], [True, True]],
    )
    result = xx.compute()["cloud_mask"].data
    assert (result == expected_result).all()

# TODO: add test for reduce - failing due to hdstats
def test_reduce(dataset):
    gm = StatsGMLSBitmask(["band_red"], "QA_PIXEL")

    xx = gm._native_tr(dataset)
    xx = gm.reduce(xx)

    xx = xx.compute()
#     print(xx["band_red"].data)
#
#     # result = xx.compute()["band_1_pc_10"].data
#     # assert (result[0, :] == 255).all()
#     # assert (result[1, :] != 255).all()
#     #
#     # expected_result = np.array(
#     #     [[1, 0], [2, 2]],
#     # )
#     # result = xx.compute()["qa"].data
#     # assert (result == expected_result).all()
#     #
#     # assert set(xx.data_vars.keys()) == set(
#     #     ["band_1_pc_10", "band_1_pc_50", "band_1_pc_90", "qa"]
#     # )