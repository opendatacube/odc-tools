import numpy as np
import xarray as xr
import dask.array as da
from odc.stats._pq_bitmask import StatsPQLSBitmask
import pytest
import pandas as pd
from copy import deepcopy
from .test_utils import usgs_ls8_sr_definition


@pytest.fixture
def dataset(usgs_ls8_sr_definition):
    band_red = np.array([
        [[255, 57], [20, 50]],
        [[30, 0], [70, 80]],
        [[25, 52], [0, 0]],
    ])
    cloud_mask = 0b0000_0000_0001_1100
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
    pq_flags_definition = {}
    for measurement in usgs_ls8_sr_definition['measurements']:
        if measurement['name'] == "QA_PIXEL":
            pq_flags_definition = measurement['flags_definition']
    attrs = dict(units="bit_index", nodata="1", crs="epsg:32633", grid_mapping="spatial_ref", flags_definition=pq_flags_definition)

    data_vars = {"band_red": (("spec", "y", "x"), band_red), "QA_PIXEL": (("spec", "y", "x"), band_pq, attrs)}
    xx = xr.Dataset(data_vars=data_vars, coords=coords, attrs=attrs)
    xx['band_red'].attrs['nodata'] = 0
    return xx

@pytest.fixture
def dataset_with_aerosol_band(dataset):
    aerosol_mask = 0b1100_0000
    no_data = 0b0000_0001
    band_aerosol = np.array([
        [[0, 0], [aerosol_mask, no_data]],
        [[no_data, 0], [0, 0]],
        [[aerosol_mask, 0], [0, 0]],
    ])

    band_aerosol = da.from_array(band_aerosol, chunks=(3, -1, -1))

    dataset["SR_QA_AEROSOL"] = (("spec", "y", "x"), band_aerosol)
    return dataset

@pytest.fixture
def dataset_with_atmos_opacity_band(dataset):
    band_atmos_opacity = np.array([
        [[0, 0], [400, -9999]],
        [[-9999, 0], [0, 0]],
        [[900, 500], [200, 0]],
    ])

    band_atmos_opacity = da.from_array(band_atmos_opacity, chunks=(3, -1, -1))

    dataset["SR_ATMOS_OPACITY"] = (("spec", "y", "x"), band_atmos_opacity)
    return dataset

def test_meaurements(dataset):
    filters = [
        {"clear_0_1_1": [("closing", 0),("opening", 1), ("dilation", 1)]},
        {"clear_1_0_1": [("closing", 1),("opening", 0), ("dilation", 1)]},
        {"clear_1_1_0": [("closing", 1),("opening", 1), ("dilation", 0)]},
        {"clear_1_1_1": [("closing", 1),("opening", 1), ("dilation", 1)]}
    ]
    aerosol_filters = [
        {"clear_0_1_1_aerosol": [("closing", 0),("opening", 1), ("dilation", 1)]},
        {"clear_1_0_1_aerosol": [("closing", 1),("opening", 0), ("dilation", 1)]}
    ]
    pq = StatsPQLSBitmask(pq_band = "QA_PIXEL", aerosol_band = "SR_QA_AEROSOL", filters=filters, aerosol_filters=aerosol_filters)

    expected_result = ("total", "clear", "clear_0_1_1", "clear_1_0_1", "clear_1_1_0", "clear_1_1_1", "clear_aerosol", "clear_0_1_1_aerosol", "clear_1_0_1_aerosol")
    measurements = pq.measurements
    assert (measurements == expected_result)

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
        [[False, False], [True, False]],
        [[False, False], [False, False]],
        [[False, True], [True, False]],
    ])
    erased_cloud = tr_result["erased"].data
    assert (erased_cloud == expected_result).all()

def test_fuser(dataset):
    pq = StatsPQLSBitmask()

    xx = pq._native_tr(dataset)
    xx = xx.groupby("solar_day").map(pq._fuser)
    fuser_result = xx.compute()

    expected_result = np.array(
        [[False, True], [True, False]],
    )
    erased_cloud = fuser_result["erased"].data
    assert (erased_cloud == expected_result).all()

def test_reduce(dataset):
    pq = StatsPQLSBitmask()

    xx = pq._native_tr(dataset)
    xx = pq.reduce(xx)
    reduce_result = xx.compute()

    assert set(reduce_result.data_vars.keys()) == set(
        ["total", "clear"]
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

def test_reduce_with_filter(dataset):
    filters = [
        {"clear_1_1": [("opening", 1), ("dilation", 1)]},
        {"clear_2_1_1": [("closing", 2), ("opening", 1), ("dilation", 1)]}
    ]
    pq = StatsPQLSBitmask(filters=filters)

    xx = pq._native_tr(dataset)
    xx = pq.reduce(xx)
    reduce_result = xx.compute()

    assert set(reduce_result.data_vars.keys()) == set(
        ["total", "clear", "clear_1_1", "clear_2_1_1"]
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
        [[2, 3], [3, 2]]
    )
    clear_1_1 = reduce_result["clear_1_1"].data
    assert (clear_1_1 == expected_result).all()

    expected_result = np.array(
        [[0, 1], [1, 1]]
    )
    clear_2_1_1 = reduce_result["clear_2_1_1"].data
    assert (clear_2_1_1 == expected_result).all()

def test_native_transform_for_aerosol(dataset_with_aerosol_band):
    pq = StatsPQLSBitmask(pq_band = "QA_PIXEL", aerosol_band = "SR_QA_AEROSOL")

    xx = pq._native_tr(dataset_with_aerosol_band)
    tr_result = xx.compute()

    expected_result = np.array([
        [[False, False], [True, False]],
        [[False, False], [False, False]],
        [[True, False], [False, False]],
    ])
    erased_aerosol = tr_result["erased_aerosol"].data
    assert (erased_aerosol == expected_result).all()

def test_fuse_for_aerosol(dataset_with_aerosol_band):
    pq = StatsPQLSBitmask(pq_band = "QA_PIXEL", aerosol_band = "SR_QA_AEROSOL")

    xx = pq._native_tr(dataset_with_aerosol_band)
    xx = xx.groupby("solar_day").map(pq._fuser)
    fuser_result = xx.compute()

    expected_result = np.array(
        [[True, False], [True, False]],
    )
    erased_aerosol = fuser_result["erased_aerosol"].data
    assert (erased_aerosol == expected_result).all()

def test_reduce_for_aerosol(dataset_with_aerosol_band):
    pq = StatsPQLSBitmask(pq_band = "QA_PIXEL", aerosol_band = "SR_QA_AEROSOL")

    xx = pq._native_tr(dataset_with_aerosol_band)
    xx = pq.reduce(xx)
    reduce_result = xx.compute()

    assert set(reduce_result.data_vars.keys()) == set(
        ["total", "clear", "clear_aerosol"]
    )

    expected_result = np.array(
        [[2, 2], [1, 2]]
    )
    clear = reduce_result["clear"].data
    assert (clear == expected_result).all()

    expected_result = np.array(
        [[1, 2], [1, 2]]
    )
    clear_aerosol = reduce_result["clear_aerosol"].data
    assert (clear_aerosol == expected_result).all()

def test_reduce_for_aerosol_with_filter(dataset_with_aerosol_band):
    filters = [{"clear_0_1_1": [("closing", 0), ("opening", 1), ("dilation", 1)]}]
    aerosol_filters = [{"clear_0_1_1": [("closing", 0), ("opening", 1), ("dilation", 1)]}]
    pq = StatsPQLSBitmask(pq_band = "QA_PIXEL", aerosol_band = "SR_QA_AEROSOL", filters=filters, aerosol_filters=aerosol_filters)

    xx = pq._native_tr(dataset_with_aerosol_band)
    xx = pq.reduce(xx)
    reduce_result = xx.compute()

    assert set(reduce_result.data_vars.keys()) == set(
        ["total", "clear", "clear_0_1_1", "clear_aerosol", "clear_0_1_1_aerosol"]
    )

    expected_result = np.array(
        [[2, 3], [3, 2]]
    )
    clear_1_1_0 = reduce_result["clear_0_1_1"].data
    assert (clear_1_1_0 == expected_result).all()

    expected_result = np.array(
        [[1, 3], [2, 2]]
    )
    clear_aerosol = reduce_result["clear_0_1_1_aerosol"].data
    assert (clear_aerosol == expected_result).all()

def test_native_transform_for_atmos_opacity(dataset_with_atmos_opacity_band):
    pq = StatsPQLSBitmask(pq_band = "QA_PIXEL", aerosol_band = "SR_ATMOS_OPACITY")

    xx = pq._native_tr(dataset_with_atmos_opacity_band)
    tr_result = xx.compute()

    expected_result = np.array([
        [[False, False], [True, False]],
        [[False, False], [False, False]],
        [[True, True], [False, False]],
    ])
    erased_aerosol = tr_result["erased_aerosol"].data
    assert (erased_aerosol == expected_result).all()

def test_fuse_for_atmos_opacity(dataset_with_atmos_opacity_band):
    pq = StatsPQLSBitmask(pq_band = "QA_PIXEL", aerosol_band = "SR_ATMOS_OPACITY")

    xx = pq._native_tr(dataset_with_atmos_opacity_band)
    xx = xx.groupby("solar_day").map(pq._fuser)
    fuser_result = xx.compute()

    expected_result = np.array(
        [[True, True], [True, False]],
    )
    erased_aerosol = fuser_result["erased_aerosol"].data
    assert (erased_aerosol == expected_result).all()

def test_reduce_for_atmos_opacity(dataset_with_atmos_opacity_band):
    pq = StatsPQLSBitmask(pq_band = "QA_PIXEL", aerosol_band = "SR_ATMOS_OPACITY")

    xx = pq._native_tr(dataset_with_atmos_opacity_band)
    xx = pq.reduce(xx)
    reduce_result = xx.compute()

    assert set(reduce_result.data_vars.keys()) == set(
        ["total", "clear", "clear_aerosol"]
    )

    expected_result = np.array(
        [[2, 2], [1, 2]]
    )
    clear = reduce_result["clear"].data
    assert (clear == expected_result).all()

    expected_result = np.array(
        [[1, 2], [1, 2]]
    )
    clear_aerosol = reduce_result["clear_aerosol"].data
    assert (clear_aerosol == expected_result).all()