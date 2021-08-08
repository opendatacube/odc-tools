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
    ]).astype(np.uint16)
    cloud_mask = 0b0000_0000_0000_1100
    no_data = 0b0000_0000_0000_0001
    band_pq = np.array([
        [[0, 0], [0, no_data]],
        [[1, 0], [0, 0]],
        [[0, cloud_mask], [0, 0]],
    ]).astype(np.uint16)

    band_red = da.from_array(band_red, chunks=(3, -1, -1))
    band_pq = da.from_array(band_pq, chunks=(3, -1, -1))

    tuples = [(np.datetime64(f"2000-01-01T0{i}"), np.datetime64(f"2000-01-01")) for i in range(3)]
    index = pd.MultiIndex.from_tuples(tuples, names=["time", "solar_day"])
    coords = {
        "x": np.linspace(10, 20, band_red.shape[2]),
        "y": np.linspace(0, 5, band_pq.shape[1]),
        "spec": index,
    }
    pq_flags_definition = {'snow': {'bits': 5, 'values': {'0': 'not_high_confidence', '1': 'high_confidence'}},
                        'clear': {'bits': 6, 'values': {'0': False, '1': True}},
                        'cloud': {'bits': 3, 'values': {'0': 'not_high_confidence', '1': 'high_confidence'}},
                        'water': {'bits': 7, 'values': {'0': 'land_or_cloud', '1': 'water'}},
                        'cirrus': {'bits': 2, 'values': {'0': 'not_high_confidence', '1': 'high_confidence'}},
                        'nodata': {'bits': 0, 'values': {'0': False, '1': True}},
                        'cloud_shadow': {'bits': 4, 'values': {'0': 'not_high_confidence', '1': 'high_confidence'}},
                        'dilated_cloud': {'bits': 1, 'values': {'0': 'not_dilated', '1': 'dilated'}},
                        'cloud_confidence': {'bits': [8, 9],
                                             'values': {'0': 'none', '1': 'low', '2': 'medium', '3': 'high'}},
                        'cirrus_confidence': {'bits': [14, 15],
                                              'values': {'0': 'none', '1': 'low', '2': 'reserved', '3': 'high'}},
                        'snow_ice_confidence': {'bits': [12, 13],
                                                'values': {'0': 'none', '1': 'low', '2': 'reserved', '3': 'high'}},
                        'cloud_shadow_confidence': {'bits': [10, 11],
                                                    'values': {'0': 'none', '1': 'low', '2': 'reserved', '3': 'high'}}}
    attrs = dict(units="bit_index", nodata="1", crs="epsg:32633", grid_mapping="spatial_ref", flags_definition=pq_flags_definition)

    data_vars = {"band_red": (("spec", "y", "x"), band_red), "QA_PIXEL": (("spec", "y", "x"), band_pq, attrs)}
    xx = xr.Dataset(data_vars=data_vars, coords=coords)
    xx['band_red'].attrs['nodata'] = 0
    return xx


def test_native_transform(dataset):
    gm = StatsGMLSBitmask(["band_red"])

    xx = gm._native_tr(dataset)
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


def test_reduce(dataset):
    _ = pytest.importorskip("hdstats")
    gm = StatsGMLSBitmask(["band_red"])

    xx = gm._native_tr(dataset)
    xx = gm.reduce(xx)

    result = xx.compute()

    assert set(xx.data_vars.keys()) == set(
        ["band_red", "smad", "emad", "bcmad", "count"]
    )

    # it's a complex calculation so we copied the result
    expected_result = np.array(
        [[59575, 59552], [59548, 59558]]
    )
    red = result["band_red"].data
    assert (red == expected_result).all()

    edev = result["emad"].data
    assert np.isclose(edev[0, 0], 32, atol=1e-6)
    assert np.isclose(edev[1, 0], 7, atol=1e-6)

    bcdev = result["bcmad"].data
    assert np.isclose(bcdev[0, 0], 0.008061964, atol=1e-6)
    assert np.isclose(bcdev[1, 0], 0.0017294621, atol=1e-6)

    sdev = result["smad"].data
    assert np.isclose(sdev[0, 0], 0.0, atol=1e-6)
    assert np.isclose(sdev[1, 0], 0.0, atol=1e-6)

    expected_result = np.array(
        [[2, 1], [2, 1]],
    )
    count = result.compute()["count"].data
    assert (count == expected_result).all()
