""" Parallel Pixel Drill
"""
from typing import Union, Iterable, Optional, Sequence, List, Any, Tuple
from ..aws.rioworkerpool import RioWorkerPool
from ..geom.pixel import make_pixel_extractor


RowCol = Tuple[int, int]
XY = Tuple[float, float]
LonLat = Tuple[float, float]
SomeCoord = Union[RowCol, XY, LonLat]


def _mode_value(pixel: Optional[RowCol] = None,
                xy: Optional[XY] = None,
                lonlat: Optional[LonLat] = None) -> Union[Tuple[str, SomeCoord],
                                                          Tuple[None, None]]:
    if pixel is not None:
        return 'pixel', pixel

    if xy is not None:
        return 'native', xy

    if lonlat is not None:
        return 'lonlat', lonlat

    return (None, None)


class PixelDrill(object):
    def __init__(self, pool: Union[RioWorkerPool, int]):
        if isinstance(pool, int):
            pool = RioWorkerPool(nthreads=pool)

        self._pool: RioWorkerPool = pool

    def read(self, urls: Iterable[str],
             pixel: Optional[RowCol] = None,
             xy: Optional[XY] = None,
             lonlat: Optional[LonLat] = None,
             band: Union[int, Sequence[int]] = 1,
             **kwargs) -> List[Any]:
        """

        urls - sequence of urls pointing to tiff images

        pixel, xy, lonlat -- Have to supply one of these three
          pixel  -- (row: int, col: int)
          xy     -- (x: float, y: float) in native coords of an image
          lonlat -- (lon: float, lat: float)

        band - Band to read

        **kwargs:

        dst_nodata - when set use that instead of defaulting to src nodata value,
                     can be set to `None` to remap to `None`

        src_nodata_fallback - nodata value to use if src file is missing nodata value

        src_nodata_override - when set use that instead of what's in the file,
                              useful when nodata metadata is incorrect in the file
                              but correct value is available out of band.
        """

        mode, coord = _mode_value(pixel=pixel, xy=xy, lonlat=lonlat)
        if mode is None:
            raise ValueError('Have to supply one of: xy, lonlat or pixel')

        extractor = make_pixel_extractor(mode=mode, band=band, **kwargs)

        def safe_extract(src):
            try:
                return extractor(src, coord)
            except ...:
                return None

        return list(self._pool.map(safe_extract, urls))
