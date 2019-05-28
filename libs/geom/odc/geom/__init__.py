""" Various Geometry Helpers

"""

from typing import Union, Tuple
from affine import Affine
import math
from datacube.utils.geometry import decompose_rws, split_translation

F4 = Tuple[float, float, float, float]
F6 = Tuple[float, float, float, float, float, float]


def maybe_zero(x: float, tol: float) -> float:
    """ Turn almost zeros to actual zeros
    """
    if abs(x) < tol:
        return 0
    return x


def maybe_int(x: float, tol: float) -> Union[int, float]:
    """ Turn almost ints to actual ints, pass through other values unmodified
    """
    def split(x):
        x_part = math.fmod(x, 1.0)
        x_whole = x - x_part
        if x_part > 0.5:
            x_part -= 1
            x_whole += 1
        elif x_part < -0.5:
            x_part += 1
            x_whole -= 1
        return (x_whole, x_part)

    x_whole, x_part = split(x)

    if abs(x_part) < tol:  # almost int
        return int(x_whole)
    else:
        return x


def _norm_grid(A: Affine, tol=1e-8) -> Union[F4, F6]:
    """Compute normalised grid.

    Different grids related via whole pixel translation will have the same
    normalised grid.

    Scale and Translation only
    --------------------------

    Normalised grid is defined by:

       sx, sy, tx, ty

    Where sx, sy is resolution (CRS units per pixel)
    and (tx, ty) is sub-pixel translation in (-0.5, 0.5)

    Order is: subpix translate -> scale

    General Case
    ------------

    Normalised grid is defined by:

       sx, sy, tx, ty, ca, w

    Order is: subpix translate -> scale -> rotate+shear

    WARNING: general case is not currently implemented
    """

    R, W, S = decompose_rws(A)

    (ca, _, tx,
     _, _, ty, *_) = R

    (_, w, *_) = W

    (sx, _, _,
     _, sy, *_) = S

    is_st = abs(ca-1) < tol and abs(w) < tol

    if is_st:
        # No rotation no shear -- commonest case
        # sx, sy, tx, ty fully defines the grid

        # Tw*S*Xp == S*Tp*Xp
        #
        # Here `p` is Pixel, `w` is World

        # convert from: <Scale pixels to World, then translate>
        # into        : <Translate pixels then scale to World>
        tx_p, ty_p = tx/sx, ty/sy

        # Remove whole pixel translation to normalise grid
        _, (tx_p, ty_p) = split_translation((tx_p, ty_p))

        return (maybe_int(sx, tol), maybe_int(sy, tol),
                maybe_zero(tx_p, tol), maybe_zero(ty_p, tol))
    else:
        raise NotImplementedError('TODO: rotated grids')
