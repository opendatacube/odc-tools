"""Helper methods for accessing single pixel from a rasterio file object.

"""

from . import resolve_nodata
import rasterio
import rasterio.warp
import rasterio.crs

NOTSET = object()


def make_pixel_extractor(mode='pixel',
                         band=1,
                         src_nodata_fallback=None,
                         src_nodata_override=None,
                         dst_nodata=NOTSET):
    """Returns function that can extract single pixel from opened rasterio file.

    Signature of returned function is:
        `src, coordinate_tuple, [band] -> pixel`

    Where coordinate_tuple is interpreted according to `mode`


    mode       - How to interpret coordinate:
                  - pixel: (row, col)
                  - native: (x, y) in file native coordinate space

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

    def extract_pixel(src, pixel_coordinate, band=default_band):
        ri, ci = pixel_coordinate

        src_nodata = resolve_nodata(src, band,
                                    fallback=src_nodata_fallback,
                                    override=src_nodata_override)

        dst_nodata = _dst_nodata(src_nodata)

        if 0 <= ri < src.height and 0 <= ci < src.width:
            window = ((ri, ri+1),
                      (ci, ci+1))

            pix = src.read(band, window=window)
            # TODO: support band being a list of bands
            return remap_pix(pix[0][0], src_nodata, dst_nodata)
        else:
            return dst_nodata

    def extract_native(src, xy, band=default_band):
        return extract_pixel(src, src.index(*xy), band=band)

    def extract_lonlat(src, lonlat, band=default_band):
        lon, lat = lonlat
        x, y = rasterio.warp.transform(rasterio.crs.CRS.from_epsg(4326), src.crs, [lon], [lat])
        xy = (x[0], y[0])
        return extract_native(src, xy, band=band)

    extractors = dict(pixel=extract_pixel,
                      native=extract_native,
                      lonlat=extract_lonlat)

    extractor = extractors.get(mode)
    if extractor is None:
        raise ValueError('Only support mode=<pixel|nativie|lonlat>')

    return extractor
