"""
"""
from affine import Affine


def decompose_rws(A):
    """Compute decomposition Affine matrix sans translation into Rotation Shear and Scale.

    Note: that there are ambiguities for negative scales.

    Example: R(90)*S(1,1) == R(-90)*S(-1,-1),
    (R*(-I))*((-I)*S) == R*S

    A = R W S

    Where:

    R [ca -sa]  W [1, w]  S [sx,  0]
      [sa  ca]    [0, 1]    [ 0, sy]

    """
    from numpy import diag, asarray
    from numpy.linalg import cholesky, det, inv

    if isinstance(A, Affine):
        def to_affine(m, t=(0, 0)):
            a, b, d, e = m.ravel()
            c, f = t
            return Affine(a, b, c,
                          d, e, f)

        (a, b, c,
         d, e, f,
         *_) = A
        R, W, S = decompose_rws(asarray([[a, b],
                                         [d, e]], dtype='float64'))

        return to_affine(R, (c, f)), to_affine(W), to_affine(S)

    assert A.shape == (2, 2)

    WS = cholesky(A.T @ A).T
    R = A @ inv(WS)

    if det(R) < 0:
        R[:, -1] *= -1
        WS[-1, :] *= -1

    ss = diag(WS)
    S = diag(ss)
    W = WS @ diag(1.0/ss)

    return R, W, S


def affine_from_pts(X, Y):
    """ Given points X,Y compute A, such that: Y = A*X.

        Needs at least 3 points.
    """
    from numpy import ones, vstack
    from numpy.linalg import lstsq

    assert len(X) == len(Y)
    assert len(X) >= 3

    n = len(X)

    XX = ones((n, 3), dtype='float64')
    YY = vstack(Y)
    for i, x in enumerate(X):
        XX[i, :2] = x

    mm, *_ = lstsq(XX, YY, rcond=None)
    a, d, b, e, c, f = mm.ravel()

    return Affine(a, b, c,
                  d, e, f)


def get_scale_at_point(pt, tr, r=None):
    """ Given an arbitrary locally linear transform estimate scale change around a point.

    1. Approximate Y = tr(X) as Y = A*X in the neighbourhood of pt
    2. Extract scale components of A


    pt - estimate transform around this point
    r  - radius around the point (default 1)

    tr - List((x,y)) -> List((x,y))
         takes list of 2-d points on input and outputs same length list of 2d on output

    """
    pts0 = [(0, 0), (-1, 0), (0, -1), (1, 0), (0, 1)]
    x0, y0 = pt
    if r is None:
        XX = [(float(x+x0), float(y+y0)) for x, y in pts0]
    else:
        XX = [(float(x*r+x0), float(y*r+y0)) for x, y in pts0]
    YY = tr(XX)
    A = affine_from_pts(XX, YY)
    _, _, S = decompose_rws(A)
    return (S.a, S.e)


def native_pix_transform(src, dst):
    """

    direction: from native for ds to geobox coords
    .back: goes the other way
    """
    from types import SimpleNamespace
    from osgeo import osr

    # TODO: special case CRS_in == CRS_out
    #
    _in = SimpleNamespace(crs=src.crs._crs, A=src.transform)
    _out = SimpleNamespace(crs=dst.crs._crs, A=dst.transform)

    _fwd = osr.CoordinateTransformation(_in.crs, _out.crs)
    _bwd = osr.CoordinateTransformation(_out.crs, _in.crs)

    _fwd = (_in.A, _fwd, ~_out.A)
    _bwd = (_out.A, _bwd, ~_in.A)

    def transform(pts, params):
        A, f, B = params
        return [B*pt[:2] for pt in f.TransformPoints([A*pt[:2] for pt in pts])]

    def tr(pts):
        return transform(pts, _fwd)
    tr.back = lambda pts: transform(pts, _bwd)

    return tr
