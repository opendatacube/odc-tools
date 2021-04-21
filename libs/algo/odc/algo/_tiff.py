import threading
from functools import partial
from dataclasses import dataclass
from typing import Union, Optional
import xarray as xr
import numpy as np
from affine import Affine
import rasterio
from rasterio.windows import Window
from rasterio import MemoryFile

NodataType = Union[int, float]


def roundup16(x):
    return (x + 15) & (~0xF)


def _adjust_blocksize(block, dim):
    if block > dim:
        return roundup16(dim)
    return roundup16(block)


@dataclass
class GeoRasterInfo:
    width: int
    height: int
    count: int
    dtype: str
    crs: str
    transform: Affine
    nodata: Optional[NodataType] = None

    def to_dict(self):
        out = dict(**self.__dict__)
        if self.nodata is None:
            out.pop("nodata")
        return out

    def raster_size(self) -> int:
        """
        Compute raster size in bytes
        """
        return np.dtype(self.dtype).itemsize * self.width * self.height * self.count

    @staticmethod
    def from_xarray(xx: xr.DataArray) -> "GeoRasterInfo":
        geobox = getattr(xx, "geobox", None)
        if geobox is None:
            raise ValueError("Missing .geobox on input array")

        height, width = geobox.shape
        if xx.ndim == 2:
            count = 1
        elif xx.ndim == 3:
            if xx.shape[:2] == (height, width):
                count = xx.shape[0]
            elif xx.shape[1:] == (height, width):
                count = xx.shape[2]
            else:
                raise ValueError("Geobox shape does not match array size")

        nodata = getattr(xx, "nodata", None)

        return GeoRasterInfo(
            width,
            height,
            count,
            xx.dtype.name,
            str(geobox.crs),
            geobox.transform,
            nodata,
        )


class TIFFSink:
    def __init__(
        self,
        info: GeoRasterInfo,
        dst: Union[str, MemoryFile],
        blocksize: Optional[int] = None,
        bigtiff="auto",
        lock=True,
        **extra_rio_opts,
    ):
        if blocksize is None:
            blocksize = 512

        if bigtiff == "auto":
            # do bigtiff if raw raster is larger than 4GB
            bigtiff = info.raster_size() > (1 << 32)

        opts = dict(
            driver="GTiff",
            bigtiff=bigtiff,
            tiled=True,
            blockxsize=_adjust_blocksize(blocksize, info.width),
            blockysize=_adjust_blocksize(blocksize, info.height),
            compress="DEFLATE",
            zlevel=6,
            predictor=2,
            num_threads="ALL_CPUS",
        )
        opts.update(info.to_dict())
        opts.update(extra_rio_opts)

        mem: Optional[MemoryFile] = None
        self._mem_mine: Optional[MemoryFile] = None

        if isinstance(dst, str):
            if dst == ":mem:":
                mem = MemoryFile()
                out = mem.open(**opts)
                self._mem_mine = mem
            else:
                out = rasterio.open(dst, mode="w", **opts)
        else:
            mem = dst
            out = dst.open(**opts)

        self._mem = mem
        self._info = info
        self._out = out
        self._lock = threading.Lock() if lock else None

    def __str__(self):
        ii = self._info
        return f"TIFFSink: {ii.width}x{ii.height}..{ii.count}..{ii.dtype}"

    def __repr__(self):
        return self.__str__()

    def close(self):
        self._out.close()

    def __del__(self):
        self.close()

        if self._mem_mine:
            self._mem_mine.close()
            self._mem_mine = None

    def __setitem__(self, key, item):
        ndim = len(key)
        info = self._info
        assert ndim in (2, 3)

        if ndim == 2:
            assert item.ndim == 2
            band, block = 1, item
            win = Window.from_slices(*key, height=info.height, width=info.width)
        elif ndim == 3:
            # TODO: figure out which dimension is "band" and which bands are being written to
            raise NotImplementedError()  # TODO:
        else:
            raise ValueError("Only accept 2 and 3 dimensional data")

        if self._lock:
            with self._lock:
                self._out.write(block, indexes=band, window=win)
        else:
            self._out.write(block, indexes=band, window=win)
