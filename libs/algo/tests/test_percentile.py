from odc.algo._percentile import np_percentile, xr_percentile
import numpy as np
import pytest
import dask.array as da
import xarray as xr


def test_np_percentile():
    arr = np.array(
        [[0, 1, 4, 6, 8, 10, 15, 22, 25, 27], [3, 5, 6, 8, 9, 11, 15, 28, 31, 50]]
    )

    np.random.shuffle(arr[0, :])
    np.random.shuffle(arr[1, :])
    arr = arr.transpose()

    assert (np_percentile(arr, 0.5, 255) == np.array([8, 9])).all()
    assert (np_percentile(arr, 0.7, 255) == np.array([15, 15])).all()
    assert (np_percentile(arr, 1.0, 255) == np.array([27, 50])).all()
    assert (np_percentile(arr, 0.0, 255) == np.array([0, 3])).all()


@pytest.mark.parametrize("nodata", [255, 200, np.nan, -1])
def test_np_percentile_some_bad_data(nodata):
    arr = np.array(
        [[0, 1, 4, 6, 8, nodata, nodata, nodata, nodata, nodata], [3, 5, 6, 8, 9, 11, 15, 28, 31, 50]]
    )

    np.random.shuffle(arr[0, :])
    np.random.shuffle(arr[1, :])
    arr = arr.transpose()

    assert (np_percentile(arr, 0.5, nodata) == np.array([4, 9])).all()
    assert (np_percentile(arr, 0.7, nodata) == np.array([6, 15])).all()
    assert (np_percentile(arr, 1.0, nodata) == np.array([8, 50])).all()
    assert (np_percentile(arr, 0.0, nodata) == np.array([0, 3])).all()


@pytest.mark.parametrize("nodata", [255, 200, np.nan])
def test_np_percentile_bad_data(nodata):
    arr = np.array(
        [
            [0, 1, nodata, nodata, nodata, nodata, nodata, nodata, nodata, nodata],
            [3, 5, 6, 8, 9, 11, 15, 28, 31, 50],
        ]
    )

    np.random.shuffle(arr[0, :])
    np.random.shuffle(arr[1, :])
    arr = arr.transpose()

    np.testing.assert_equal(np_percentile(arr, 0.5, nodata), np.array([nodata, 9]))
    np.testing.assert_equal(np_percentile(arr, 0.7, nodata), np.array([nodata, 15]))
    np.testing.assert_equal(np_percentile(arr, 1.0, nodata), np.array([nodata, 50]))
    np.testing.assert_equal(np_percentile(arr, 0.0, nodata), np.array([nodata, 3]))


@pytest.mark.parametrize("nodata", [255, 200, np.nan, -1]) #should do -1
def test_xr_percentile(nodata):
    band_1 = np.random.randint(0, 100, size=(10, 100, 200)).astype(type(nodata))
    band_2 = np.random.randint(0, 100, size=(10, 100, 200)).astype(type(nodata))

    band_1[np.random.random(size=band_1.shape) > 0.5] = nodata
    band_2[np.random.random(size=band_1.shape) > 0.5] = nodata

    true_results = dict()
    true_results["band_1_pc_20"] = np_percentile(band_1, 0.2, nodata)
    true_results["band_2_pc_20"] = np_percentile(band_2, 0.2, nodata)
    true_results["band_1_pc_60"] = np_percentile(band_1, 0.6, nodata)
    true_results["band_2_pc_60"] = np_percentile(band_2, 0.6, nodata)

    band_1 = da.from_array(band_1, chunks=(2, 20, 20))
    band_2 = da.from_array(band_2, chunks=(2, 20, 20))

    attrs = {"test": "attrs"}
    coords = {
        "x": np.linspace(10, 20, band_1.shape[2]), 
        "y": np.linspace(0, 5, band_1.shape[1]), 
        "t": np.linspace(0, 5, band_1.shape[0])
    }

    data_vars = {
        "band_1": xr.DataArray(band_1, dims=("t", "y", "x"), attrs={"test_attr": 1}),
        "band_2": xr.DataArray(band_2, dims=("t", "y", "x"), attrs={"test_attr": 2}),
    }

    dataset = xr.Dataset(data_vars=data_vars, coords=coords, attrs=attrs)
    output = xr_percentile(dataset, [0.2, 0.6], nodata).compute()

    for key in output.keys():
        np.testing.assert_equal(output[key], true_results[key])
    
    assert output["band_1_pc_20"].attrs["test_attr"] == 1
    assert output["band_1_pc_60"].attrs["test_attr"] == 1
    assert output["band_2_pc_20"].attrs["test_attr"] == 2
    assert output["band_2_pc_20"].attrs["test_attr"] == 2