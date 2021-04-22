import threading
from pathlib import Path
from dataclasses import dataclass
from typing import Optional, Union, Tuple
import xarray as xr
import numpy as np
from affine import Affine
import rasterio
from uuid import uuid4
from rasterio.windows import Window
from rasterio import MemoryFile
from rasterio.shutil import copy as rio_copy

from ._types import NodataType, NumpyIndex
from ._numeric import roundup16, half_up, roi_shrink2, np_slice_to_idx
from ._warp import _shrink2


def _adjust_blocksize(block: int, dim: int) -> int:
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
    axis: int = 0

    def gdal_opts(self):
        out = dict(**self.__dict__)
        if self.nodata is None:
            out.pop("nodata")
        out.pop("axis")
        return out

    def raster_size(self) -> int:
        """
        Compute raster size in bytes
        """
        return np.dtype(self.dtype).itemsize * self.width * self.height * self.count

    @staticmethod
    def from_xarray(xx: xr.DataArray) -> "GeoRasterInfo":
        axis = 0
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
                axis = 1
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
            axis=axis,
        )

    def shrink2(self) -> "GeoRasterInfo":
        return GeoRasterInfo(
            width=half_up(self.width),
            height=half_up(self.height),
            count=self.count,
            dtype=self.dtype,
            crs=self.crs,
            transform=self.transform * Affine.scale(2, 2),
            nodata=self.nodata,
        )


class TIFFSink:
    def __init__(
        self,
        info: GeoRasterInfo,
        dst: Union[str, MemoryFile],
        blocksize: Optional[int] = None,
        bigtiff: Union[str, bool] = "auto",
        lock: bool = True,
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
        opts.update(info.gdal_opts())
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

    def __str__(self) -> str:
        ii = self._info
        return f"TIFFSink: {ii.width}x{ii.height}..{ii.count}..{ii.dtype}"

    def __repr__(self) -> str:
        return self.__str__()

    @property
    def name(self) -> str:
        return self._out.name

    @property
    def info(self) -> GeoRasterInfo:
        return self._info

    def close(self):
        self._out.close()

    def __del__(self):
        self.close()

        if self._mem_mine:
            self._mem_mine.close()
            self._mem_mine = None

    def __setitem__(self, key: NumpyIndex, item: np.ndarray):
        ndim = len(key)
        info = self._info
        assert ndim in (2, 3)

        yx_key = key[info.axis : info.axis + 2]
        if ndim == 2:
            assert info.axis == 0
            assert item.ndim == 2
            bands: Union[int, Tuple[int, ...]] = 1
            block = item
        elif ndim == 3:
            if info.axis == 0:
                # Y, X, B
                bands = np_slice_to_idx(key[2], info.count)
                if item.ndim == 2:
                    block = np.expand_dims(item, axis=0)
                else:
                    # rio expects band to be the first dimension
                    block = item.transpose([2, 0, 1])
            else:
                # B, Y, X
                bands = np_slice_to_idx(key[0], info.count)
                if item.ndim == 2:
                    block = np.expand_dims(item, axis=0)
                else:
                    block = item

            # rio wants 1 based indexing
            bands = tuple(i + 1 for i in bands)
        else:
            raise ValueError("Only accept 2 and 3 dimensional data")

        win = Window.from_slices(*yx_key, height=info.height, width=info.width)
        if self._lock:
            with self._lock:
                self._out.write(block, indexes=bands, window=win)
        else:
            self._out.write(block, indexes=bands, window=win)


class COGSink:
    def __init__(
        self,
        info: GeoRasterInfo,
        dst: str,
        blocksize: Optional[int] = None,
        ovr_blocksize: Optional[int] = None,
        bigtiff: Union[bool, str] = "auto",
        lock: bool = True,
        temp_folder: Optional[str] = None,
        overview_resampling: str = "average",
        **extra_rio_opts,
    ):
        if blocksize is None:
            blocksize = 512

        if ovr_blocksize is None:
            ovr_blocksize = blocksize

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
        opts.update(extra_rio_opts)

        rio_opts_temp = dict(
            compress="zstd",
            zstd_level=1,
            predictor=1,
            num_threads="ALL_CPUS",
            sparse_ok=True,
        )
        layers = []
        temp = str(uuid4())
        t_dir = ""
        if temp_folder:
            t_name = temp
        else:
            t_dir, t_name = temp[:8], temp[9:]

        ext = ".tif"
        ii = info
        bsz = 2048
        for _ in range(7 + 1):
            if temp_folder:
                _dst = str(Path(temp_folder) / f"{t_name}{ext}")
            else:
                _dst = MemoryFile(dirname=t_dir, filename=t_name + ext)
            sink = TIFFSink(
                ii, _dst, lock=lock, blocksize=bsz, bigtiff=bigtiff, **rio_opts_temp
            )
            layers.append(sink)

            # If last overview was smaller than 1 block along any dimension don't
            # go further
            if min(ii.width, ii.height) < ovr_blocksize:
                break

            ii = ii.shrink2()
            ext = ext + ".ovr"
            if bsz > 64:
                bsz = bsz // 2

        self._layers = layers
        self._dst = dst
        self._rio_opts = opts
        self._ovr_blocksize = ovr_blocksize
        self._resampling = overview_resampling
        self._info = info

    def _shrink2(self, xx, roi):
        axis = self._info.axis
        out_roi = roi_shrink2(roi, axis=axis)
        out = _shrink2(
            xx, resampling=self._resampling, nodata=self._info.nodata, axis=axis
        )

        return out_roi, out

    def __setitem__(self, key: NumpyIndex, item: np.ndarray):
        dst, *ovrs = self._layers
        dst[key] = item
        for dst in ovrs:
            key, item = self._shrink2(item, key)
            dst[key] = item

    def close(self):
        for dst in self._layers:
            dst.close()

    def finalise(self) -> Optional[bytes]:
        self.close()  # Write out any remainders if needed

        with rasterio.Env(GDAL_TIFF_OVR_BLOCKSIZE=self._ovr_blocksize):
            src = self._layers[0].name
            if self._dst == ":mem:":
                with MemoryFile() as mem:
                    rio_copy(src, mem.name, copy_src_overviews=True, **self._rio_opts)
                    return bytes(mem.getbuffer())
            else:
                rio_copy(src, self._dst, copy_src_overviews=True, **self._rio_opts)
                return None
