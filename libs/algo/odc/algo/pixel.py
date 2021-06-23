"""Helper methods for accessing single pixel from a rasterio file object.

"""
import rasterio
import rasterio.warp
import rasterio.crs

from typing import Union, Iterable, Optional, List, Tuple

RowCol = Tuple[int, int]
XY = Tuple[float, float]
LonLat = Tuple[float, float]
SomeCoord = Union[RowCol, XY, LonLat]
PixelValue = Union[float, int]


NOTSET = object()


def make_pixel_extractor(
    mode="pixel",
    band=1,
    src_nodata_fallback=None,
    src_nodata_override=None,
    dst_nodata=NOTSET,
):
    """Returns function that can extract single pixel from opened rasterio file.

    Signature of the returned function is:
        `src, coordinate_tuple, [band] -> pixel`

    Where coordinate_tuple is interpreted according to `mode`


    mode       - How to interpret coordinate:
                  - pixel: (row, col)
                  - native: (x, y) in file native coordinate space
                  - lonlat: (lon, lat)  (specifically EPSG:4326)

    band       - Default band to read, can be over-written on read

    dst_nodata - when set use that instead of defaulting to src nodata value,
                 can be set to `None` to remap to `None`

    src_nodata_fallback - nodata value to use if src file is missing nodata value

    src_nodata_override - when set use that instead of what's in the file,
                          useful when nodata metadata is incorrect in the file
                          but correct value is available out of band.

    """
    default_band = band

    if dst_nodata is NOTSET:

        def _dst_nodata(src_nodata):
            return src_nodata

    else:

        def _dst_nodata(src_nodata):
            return dst_nodata

    def remap_pix(pix, src_nodata, dst_nodata):
        # TODO: special case src_nodata is nan case
        return dst_nodata if pix == src_nodata else pix

    def extract_pixel(src, coord, band=default_band):
        ri, ci = coord

        src_nodata = _resolve_nodata(
            src, band, fallback=src_nodata_fallback, override=src_nodata_override
        )

        dst_nodata = _dst_nodata(src_nodata)

        if 0 <= ri < src.height and 0 <= ci < src.width:
            window = ((ri, ri + 1), (ci, ci + 1))

            pix = src.read(band, window=window)
            # TODO: support band being a list of bands
            return remap_pix(pix[0][0], src_nodata, dst_nodata)
        else:
            return dst_nodata

    def extract_native(src, coord, band=default_band):
        return extract_pixel(src, src.index(*coord), band=band)

    def extract_lonlat(src, coord, band=default_band):
        lon, lat = coord
        x, y = rasterio.warp.transform(
            rasterio.crs.CRS.from_epsg(4326), src.crs, [lon], [lat]
        )
        xy = (x[0], y[0])
        return extract_native(src, xy, band=band)

    extractors = dict(pixel=extract_pixel, native=extract_native, lonlat=extract_lonlat)

    extractor = extractors.get(mode)
    if extractor is None:
        raise ValueError("Only support mode=<pixel|native|lonlat>")

    return extractor


def _resolve_nodata(src, band, fallback=None, override=None):
    """Figure out what value to use for nodata given a band and fallback/override
    settings

    :param src: Rasterio file
    """
    if override is not None:
        return override

    band0 = band if isinstance(band, int) else band[0]
    nodata = src.nodatavals[band0 - 1]

    if nodata is None:
        return fallback

    return nodata


def _mode_value(
    pixel: Optional[RowCol] = None,
    xy: Optional[XY] = None,
    lonlat: Optional[LonLat] = None,
) -> Union[Tuple[str, SomeCoord], Tuple[None, None]]:
    if pixel is not None:
        return "pixel", pixel

    if xy is not None:
        return "native", xy

    if lonlat is not None:
        return "lonlat", lonlat

    return (None, None)


def read_pixels(
    urls: Iterable[str],
    pixel: Optional[RowCol] = None,
    xy: Optional[XY] = None,
    lonlat: Optional[LonLat] = None,
    band: int = 1,
    **kwargs
) -> List[PixelValue]:
    """Read a single pixel at the same location from a bunch of different files.

    Location can be specified in 3 different ways:

      pixel  (row: int, column: int)  -- in pixel coords
      xy     (X: float, Y: float)     -- in Projected coordinates of the native CRS of the image
      lonlat (lon: float, lat: float) -- in EPSG:4326
    """
    mode, coord = _mode_value(pixel=pixel, xy=xy, lonlat=lonlat)
    if mode is None:
        raise ValueError("Have to supply one of: pixel, xy, or lonlat.")

    extractor = make_pixel_extractor(mode=mode, band=band, **kwargs)

    def read_from_url(url):
        url = rasterio.parse_path(url)
        with rasterio.DatasetReader(url, sharing=False) as src:
            return extractor(src, coord=coord)

    return [read_from_url(url) for url in urls]
