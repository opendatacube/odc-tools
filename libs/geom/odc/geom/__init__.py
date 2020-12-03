""" Various Geometry Helpers

"""

from typing import Union, Tuple, Optional
from affine import Affine
import math
from datacube.utils.geometry import (
    decompose_rws,
    split_translation,
    GeoBox,
    CRS,
    SomeCRS)
from datacube.utils.geometry._base import _norm_crs_or_error

F4 = Tuple[float, float, float, float]
F6 = Tuple[float, float, float, float, float, float]


class BoundlessPixelPlane:
    def __init__(self, crs: CRS, params: Union[F4, F6]):
        """
        :param crs: CRS
        :param params: Normalized grid coefficients:
                       sx, sy, tx, ty[, ca, w]
        """

        self._crs = crs
        self._params = params
        self._epsg = None if crs is None else crs.epsg

    def same(self, other, tol=1e-6):
        if len(self._params) != len(other._params):
            return False

        if self._epsg is not None:
            if self._epsg != other._epsg:
                return False
        elif self._crs != other._crs:
            return False

        return all(abs(a-b) < tol for (a, b) in zip(self._params, other._params))

    def __eq__(self, other):
        return self.same(other)

    def __repr__(self):
        return str(self)

    def __str__(self):
        if self._epsg:
            crs_str = 'EPSG:{}'.format(self._epsg)
        else:
            crs_str = str(self._crs)

        p_str = ','.join([str(p) for p in self._params])

        return 'BoundlessPixelPlane<{}, {}>'.format(crs_str, p_str)


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


def normalised_grid(geobox: GeoBox) -> BoundlessPixelPlane:
    """Compute normalised grid from a given GeoBox.

       Two different GridBoxes that are related through pixel aligned
       translation will produce the same normalised grid.
    """
    return BoundlessPixelPlane(geobox.crs,
                               _norm_grid(geobox.affine))


def gbox_reproject(geobox: GeoBox,
                   crs: SomeCRS,
                   resolution: Optional[Tuple[int, int]] = None,
                   pad: int = 0,
                   pad_wh: Union[int, Tuple[int, int]] = 16) -> GeoBox:
    """
    Compute GeoBox in a given projection that fully encloses footprint of the source GeoBox.

    :param geobox: Source GeoBox

    :param crs: CRS of the output GeoBox

    :param resolution: Desired output resolution (defaults to source
                       resolution, if source and destination projections share
                       common units)

    :param pad: Padding in pixels of output GeoBox

    :param pad_wh: Expand output GeoBox some more such that (W%pad_wh) == 0 and (H%pad_wh) == 0
    """
    from datacube.utils.geometry import gbox

    crs = _norm_crs_or_error(crs)

    if resolution is None:
        if geobox.crs.units == crs.units:
            resolution = geobox.resolution
        else:
            raise NotImplementedError(
                "Source and destination projections have different units: have to supply desired output resolution"
            )

    out = GeoBox.from_geopolygon(geobox.extent, resolution, crs)
    if pad > 0:
        out = gbox.pad(out, pad)
    if pad_wh:
        if isinstance(pad_wh, int):
            pad_wh = max(1, pad_wh)
            pad_wh = (pad_wh, pad_wh)
        out = gbox.pad_wh(out, *pad_wh)

    return out
