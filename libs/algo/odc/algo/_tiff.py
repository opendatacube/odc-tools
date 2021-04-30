import threading
from dask.delayed import Delayed
import dask
import dask.array as da
from dask.base import tokenize
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
                count = xx.shape[2]
            elif xx.shape[1:] == (height, width):
                count = xx.shape[0]
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

        with rasterio.Env(
            GDAL_TIFF_OVR_BLOCKSIZE=self._ovr_blocksize,
            GDAL_DISABLE_READDIR_ON_OPEN=False,
        ):
            src = self._layers[0].name
            if self._dst == ":mem:":
                with MemoryFile() as mem:
                    rio_copy(src, mem.name, copy_src_overviews=True, **self._rio_opts)
                    return bytes(mem.getbuffer())
            else:
                rio_copy(src, self._dst, copy_src_overviews=True, **self._rio_opts)
                return None


def save_cog(
    xx: xr.DataArray,
    dst: str,
    blocksize: Optional[int] = None,
    ovr_blocksize: Optional[int] = None,
    bigtiff: Union[bool, str] = "auto",
    temp_folder: Optional[str] = None,
    overview_resampling: str = "average",
    **extra_rio_opts,
):
    """
    Save Dask array to COG incrementally (without instantiating whole image at once).

    COG generation is a two stage process. First we create a bunch of TIFF
    images, one for each overview levels, these are compressed with fast ZSTD
    compression (lossless and with quick rather than good compression
    settings). Overviews are generated block by block so we do not keep them
    in-memory uncompressed. To avoid block boundary artefacts, input blocks are
    set to be 2048x2048 (2**11, edge blocks can have any size). Use that size
    at construction time for optimal performance.

    :param xx: Geo registered Array, data could be arranged in Y,X or Y,X,B or B,Y,X order.
               To avoid re-chunking use block sizes of 2048x2048.
    :param dst: ":mem:" or file path
    :param blocksize: Block size of the final COG (512 pixels)
    :param ovr_blocksize: Block size of the overview images (default same as main image)
    :param bigtiff: True|False|"auto" Default is to use bigtiff for inputs greater than 4Gb uncompressed
    :param temp_folder: By default first pass images are written to RAM, with this option they can be written to disk instead
    :param overview_resampling: Resampling to use for overview generation: nearest|average|bilinear|...
    """
    assert dask.is_dask_collection(xx)
    tk = tokenize(
        xx,
        dst,
        blocksize,
        ovr_blocksize,
        bigtiff,
        temp_folder,
        overview_resampling,
        extra_rio_opts,
    )

    info = GeoRasterInfo.from_xarray(xx)

    # Rechunk to 2048x2048 in YX, if needed
    axis = info.axis
    data = xx.data
    chunks = data.chunksize
    yx_chunks = chunks[axis : axis + 2]

    if yx_chunks != (2048, 2048):
        data = data.rechunk(chunks[:axis] + (2048, 2048) + chunks[axis + 2 :])

    # set up sink
    sink = dask.delayed(COGSink)(
        info,
        dst,
        blocksize=blocksize,
        ovr_blocksize=ovr_blocksize,
        bigtiff=bigtiff,
        temp_folder=temp_folder,
        overview_resampling=overview_resampling,
        **extra_rio_opts,
    )

    rr = da.store(data, sink, lock=False, compute=False)
    dsk = dict(rr.dask)
    deps = dsk.pop(rr.key)
    name = "cog-" + tk

    # when all stores are complete, do second pass re-encode
    dsk[name] = (lambda sink, *deps: sink.finalise(), sink.key, *deps)
    return Delayed(name, dsk)
