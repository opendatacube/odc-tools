import numpy as np
import xarray as xr
import dask
import dask.array as da
import pytest

from odc.algo._masking import (
    _gap_fill_np,
    gap_fill,
    fmask_to_bool,
    enum_to_bool,
    _get_enum_values,
    _enum_to_mask_numexpr,
    _fuse_mean_np,
    mask_cleanup_np
)


def test_gap_fill():
    a = np.zeros((5,), dtype="uint8")
    b = np.empty_like(a)
    b[:] = 33

    a[0] = 11
    ab = _gap_fill_np(a, b, 0)
    assert ab.dtype == a.dtype
    assert ab.tolist() == [11, 33, 33, 33, 33]

    xa = xr.DataArray(
        a,
        name="test_a",
        dims=("t",),
        attrs={"p1": 1, "nodata": 0},
        coords=dict(t=np.arange(a.shape[0])),
    )
    xb = xa + 0
    xb.data[:] = b
    xab = gap_fill(xa, xb)
    assert xab.name == xa.name
    assert xab.attrs == xa.attrs
    assert xab.data.tolist() == [11, 33, 33, 33, 33]

    xa.attrs["nodata"] = 11
    assert gap_fill(xa, xb).data.tolist() == [33, 0, 0, 0, 0]

    a = np.zeros((5,), dtype="float32")
    a[1:] = np.nan
    b = np.empty_like(a)
    b[:] = 33
    ab = _gap_fill_np(a, b, np.nan)

    assert ab.dtype == a.dtype
    assert ab.tolist() == [0, 33, 33, 33, 33]

    xa = xr.DataArray(
        a,
        name="test_a",
        dims=("t",),
        attrs={"p1": 1},
        coords=dict(t=np.arange(a.shape[0])),
    )
    xb = xa + 0
    xb.data[:] = b
    xab = gap_fill(xa, xb)
    assert xab.name == xa.name
    assert xab.attrs == xa.attrs
    assert xab.data.tolist() == [0, 33, 33, 33, 33]

    xa = xr.DataArray(
        da.from_array(a),
        name="test_a",
        dims=("t",),
        attrs={"p1": 1},
        coords=dict(t=np.arange(a.shape[0])),
    )

    xb = xr.DataArray(
        da.from_array(b),
        name="test_a",
        dims=("t",),
        attrs={"p1": 1},
        coords=dict(t=np.arange(b.shape[0])),
    )

    assert dask.is_dask_collection(xa)
    assert dask.is_dask_collection(xb)
    xab = gap_fill(xa, xb)

    assert dask.is_dask_collection(xab)
    assert xab.name == xa.name
    assert xab.attrs == xa.attrs
    assert xab.compute().values.tolist() == [0, 33, 33, 33, 33]


def test_fmask_to_bool():
    def _fake_flags(prefix="cat_", n=65):
        return dict(
            bits=list(range(8)), values={str(i): f"{prefix}{i}" for i in range(0, n)}
        )

    flags_definition = dict(fmask=_fake_flags())

    fmask = xr.DataArray(
        np.arange(0, 65, dtype="uint8"), attrs=dict(flags_definition=flags_definition)
    )

    mm = fmask_to_bool(fmask, ("cat_1", "cat_3"))
    (ii,) = np.where(mm)
    assert tuple(ii) == (1, 3)

    # upcast to uint16 internally
    mm = fmask_to_bool(fmask, ("cat_0", "cat_15"))
    (ii,) = np.where(mm)
    assert tuple(ii) == (0, 15)

    # upcast to uint32 internally
    mm = fmask_to_bool(fmask, ("cat_1", "cat_3", "cat_31"))
    (ii,) = np.where(mm)
    assert tuple(ii) == (1, 3, 31)

    # upcast to uint64 internally
    mm = fmask_to_bool(fmask, ("cat_0", "cat_32", "cat_37", "cat_63"))
    (ii,) = np.where(mm)
    assert tuple(ii) == (0, 32, 37, 63)

    with pytest.raises(ValueError):
        fmask_to_bool(fmask, ("cat_64"))

    mm = fmask_to_bool(fmask.chunk(3), ("cat_0",)).compute()
    (ii,) = np.where(mm)
    assert tuple(ii) == (0,)

    mm = fmask_to_bool(fmask.chunk(3), ("cat_31", "cat_63")).compute()
    (ii,) = np.where(mm)
    assert tuple(ii) == (31, 63)

    # check _get_enum_values
    flags_definition = dict(cat=_fake_flags("cat_"), dog=_fake_flags("dog_"))
    assert _get_enum_values(("cat_0",), flags_definition) == (0,)
    assert _get_enum_values(("cat_0", "cat_12"), flags_definition) == (0, 12)
    assert _get_enum_values(("dog_0", "dog_13"), flags_definition) == (0, 13)
    assert _get_enum_values(("dog_0", "dog_13"), flags_definition, flag="dog") == (
        0,
        13,
    )

    with pytest.raises(ValueError) as e:
        _get_enum_values(("cat_10", "_nope"), flags_definition)
    assert "Can not find flags definitions" in str(e)

    with pytest.raises(ValueError) as e:
        _get_enum_values(("cat_10", "bah", "dog_0"), flags_definition, flag="dog")
    assert "cat_10" in str(e)


def test_enum_to_mask():
    nmax = 129

    def _fake_flags(prefix="cat_", n=nmax + 1):
        return dict(
            bits=list(range(8)), values={str(i): f"{prefix}{i}" for i in range(0, n)}
        )

    flags_definition = dict(fmask=_fake_flags())

    fmask_no_flags = xr.DataArray(np.arange(0, nmax + 1, dtype="uint16"))
    fmask = xr.DataArray(
        np.arange(0, nmax + 1, dtype="uint16"),
        attrs=dict(flags_definition=flags_definition),
    )

    mm = enum_to_bool(fmask, ("cat_1", "cat_3", nmax, 33))
    (ii,) = np.where(mm)
    assert tuple(ii) == (1, 3, 33, nmax)

    mm = enum_to_bool(fmask, (0, 3, 17))
    (ii,) = np.where(mm)
    assert tuple(ii) == (0, 3, 17)

    mm = enum_to_bool(fmask_no_flags, (0, 3, 17))
    (ii,) = np.where(mm)
    assert tuple(ii) == (0, 3, 17)
    assert mm.dtype == "bool"

    mm = enum_to_bool(fmask_no_flags, (0, 3, 8, 17), dtype="uint8", value_true=255)
    (ii,) = np.where(mm == 255)
    assert tuple(ii) == (0, 3, 8, 17)
    assert mm.dtype == "uint8"

    mm = enum_to_bool(
        fmask_no_flags, (0, 3, 8, 17), dtype="uint8", value_true=255, invert=True
    )
    (ii,) = np.where(mm != 255)
    assert tuple(ii) == (0, 3, 8, 17)
    assert mm.dtype == "uint8"


def test_enum_to_mask_numexpr():
    elements = (1, 4, 23)
    mm = np.asarray([1, 2, 3, 4, 5, 23], dtype="uint8")

    np.testing.assert_array_equal(
        _enum_to_mask_numexpr(mm, elements), np.isin(mm, elements)
    )
    np.testing.assert_array_equal(
        _enum_to_mask_numexpr(mm, elements, invert=True),
        np.isin(mm, elements, invert=True),
    )

    bb8 = _enum_to_mask_numexpr(mm, elements, dtype="uint8", value_true=255)
    assert bb8.dtype == "uint8"

    np.testing.assert_array_equal(
        _enum_to_mask_numexpr(mm, elements, dtype="uint8", value_true=255) == 255,
        np.isin(mm, elements),
    )


def test_fuse_mean_np():
    data = np.array([
        [[255, 255], [255, 50]],
        [[30, 40], [255, 80]],
        [[25, 52], [255, 98]],
    ]).astype(np.uint8)

    slices = [data[i:i+1] for i in range(data.shape[0])]
    out = _fuse_mean_np(*slices, nodata=255)
    assert (out == np.array([[28, 46], [255, 76]])).all()


def test_mask_cleanup_np():
    mask = np.ndarray(shape=(2,2), dtype=bool, buffer=np.array([[True, False], [False, True]]))

    mask_filter_with_opening_dilation = [("opening", 1), ("dilation", 1)]
    result = mask_cleanup_np(mask, mask_filter_with_opening_dilation)
    expected_result = np.array(
        [[False, False], [False, False]],
    )
    assert (result == expected_result).all()

    mask_filter_opening = [("opening", 1), ("dilation", 0)]
    result = mask_cleanup_np(mask, mask_filter_opening)
    expected_result = np.array(
        [[False, False], [False, False]],
    )
    assert (result == expected_result).all()

    mask_filter_with_dilation = [("opening", 0), ("dilation", 1)]
    result = mask_cleanup_np(mask, mask_filter_with_dilation)
    expected_result = np.array(
        [[True, True], [True, True]],
    )
    assert (result == expected_result).all()

    mask_filter_with_closing = [("closing", 1), ("opening", 1), ("dilation", 1)]
    result = mask_cleanup_np(mask, mask_filter_with_closing)
    expected_result = np.array(
        [[True, True], [True, True]],
    )
    assert (result == expected_result).all()

    mask_filter_with_all_zero = [("closing", 0), ("opening", 0), ("dilation", 0)]
    result = mask_cleanup_np(mask, mask_filter_with_all_zero)
    expected_result = np.array(
        [[True, False], [False, True]],
    )
    assert (result == expected_result).all()

    invalid_mask_filter = [("oppening", 1), ("dilation", 1)]
    with pytest.raises(Exception):
        mask_cleanup_np(mask, invalid_mask_filter)