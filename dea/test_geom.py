from affine import Affine
from math import sqrt
from random import uniform
from .geom import affine_from_pts, decompose_rws, scaled_down_geobox


def mkA(rot=0, scale=(1, 1), shear=0, translation=(0, 0)):
    return Affine.translation(*translation)*Affine.rotation(rot)*Affine.shear(shear)*Affine.scale(*scale)


def get_diff(A, B):
    return sqrt(sum((a-b)**2 for a, b in zip(A, B)))


def test_rsw():
    def run_test(a, scale, shear=0, translation=(0, 0), tol=1e-8):
        A = mkA(a, scale=scale, shear=shear, translation=translation)

        R, W, S = decompose_rws(A)

        assert get_diff(A, R*W*S) < tol
        assert get_diff(S, mkA(0, scale)) < tol
        assert get_diff(R, mkA(a, translation=translation)) < tol

    for a in (0, 12, 45, 33, 67, 89, 90, 120, 170):
        run_test(a, (1, 1))
        run_test(a, (0.5, 2))
        run_test(-a, (0.5, 2))

        run_test(a, (1, 2))
        run_test(-a, (1, 2))

        run_test(a, (2, -1))
        run_test(-a, (2, -1))

    run_test(0, (3, 4), 10)
    run_test(-33, (3, -1), 10, translation=(100, -333))


def test_fit():
    def run_test(A, n, tol=1e-5):
        X = [(uniform(0, 1), uniform(0, 1))
             for _ in range(n)]
        Y = [A*x for x in X]
        A_ = affine_from_pts(X, Y)

        assert get_diff(A, A_) < tol

    A = mkA(13, scale=(3, 4), shear=3, translation=(100, -3000))

    run_test(A, 3)
    run_test(A, 10)

    run_test(mkA(), 3)
    run_test(mkA(), 10)


def test_geobox():
    from datacube.utils.geometry import GeoBox, CRS

    crs = CRS('EPSG:3857')

    A = mkA(0, (111.2, 111.2), translation=(125_671, 251_465))
    for s in [2, 3, 4, 8, 13, 16]:
        gbox = GeoBox(233*s, 755*s, A, crs)
        gbox_ = scaled_down_geobox(gbox, s)

        assert gbox_.width == 233
        assert gbox_.height == 755
        assert gbox_.crs is crs
        assert gbox_.extent.contains(gbox.extent)
        assert gbox.extent.difference(gbox.extent).area == 0.0

    gbox = GeoBox(1, 1, A, crs)
    for s in [2, 3, 5]:
        gbox_ = scaled_down_geobox(gbox, 3)

        assert gbox_.shape == (1, 1)
        assert gbox_.crs is crs
        assert gbox_.extent.contains(gbox.extent)


def test_roi():
    from .geom import roi_is_empty, roi_shape
    from numpy import s_

    assert roi_shape(s_[2:4, 3:4]) == (2, 1)
    assert roi_shape(s_[:4, :7]) == (4, 7)

    assert roi_is_empty(s_[:4, :5]) is False
    assert roi_is_empty(s_[1:1, :10]) is True
    assert roi_is_empty(s_[7:3, :10]) is True
