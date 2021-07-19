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
    cloud_mask = 0b0000_0000_0001_1010
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
        [[False, False], [False, False]],
        [[False, False], [False, False]],
        [[False, True], [False, False]],
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
        [[False, True], [False, False]],
    )
    result = xx.compute()["cloud_mask"].data
    assert (result == expected_result).all()

@pytest.mark.parametrize("return_SR", [True, False])
def test_reduce(dataset, return_SR):
    gm = StatsGMLSBitmask(["band_red"], return_SR=return_SR)

    xx = gm._native_tr(dataset)
    xx = gm.reduce(xx)

    result = xx.compute()

    assert set(xx.data_vars.keys()) == set(
        ["band_red", "sdev", "edev", "bcdev", "count"]
    )

    # it's a complex calculation so we copied the result
    if return_SR:
        expected_result = np.array(
            [[59575, 59552], [59548, 59558]]
        )
        red = result["band_red"].data
        assert (red == expected_result).all()
    else:
        expected_result = np.array(
            [[-14405, -14488], [-14500, -14465]]
        )
        red = result["band_red"].data
        assert (red == expected_result).all()

    edev = result["edev"].data
    assert np.isclose(edev[0, 0], 32, atol=1e-6)
    assert np.isclose(edev[1, 0], 7, atol=1e-6)

    bcdev = result["bcdev"].data
    assert np.isclose(bcdev[0, 0], 0.008061964, atol=1e-6)
    assert np.isclose(bcdev[1, 0], 0.0017294621, atol=1e-6)

    sdev = result["sdev"].data
    assert np.isclose(sdev[0, 0], 0.0, atol=1e-6)
    assert np.isclose(sdev[1, 0], 0.0, atol=1e-6)

    expected_result = np.array(
        [[2, 1], [2, 1]],
    )
    count = result.compute()["count"].data
    assert (count == expected_result).all()