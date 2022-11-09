""" Various Algorithmic Helpers

"""

from ._broadcast import pool_broadcast
from ._dask import (
    chunked_persist,
    chunked_persist_da,
    chunked_persist_ds,
    randomize,
    reshape_yxbt,
    wait_for_future,
)
from ._dask_stream import dask_compute_stream, seq_to_bags
from ._geomedian import (
    geomedian_with_mads,
    int_geomedian,
    int_geomedian_np,
    reshape_for_geomedian,
    xr_geomedian,
)
from ._masking import (
    binary_closing,
    binary_dilation,
    binary_erosion,
    binary_opening,
    choose_first_valid,
    enum_to_bool,
    erase_bad,
    fmask_to_bool,
    from_float,
    from_float_np,
    gap_fill,
    keep_good_np,
    keep_good_only,
    mask_cleanup,
    mask_cleanup_np,
    to_f32,
    to_f32_np,
    to_float,
    to_float_np,
)
from ._memsink import (
    da_mem_sink,
    da_yxbt_sink,
    da_yxt_sink,
    store_to_mem,
    yxbt_sink,
    yxbt_sink_to_mem,
    yxt_sink,
)
from ._numexpr import apply_numexpr, safe_div
from ._percentile import xr_quantile
from ._rgba import colorize, is_rgb, to_rgba, to_rgba_np
from ._tiff import save_cog
from ._version import __version__
from ._warp import xr_reproject

__all__ = (
    "apply_numexpr",
    "safe_div",
    "keep_good_np",
    "keep_good_only",
    "erase_bad",
    "from_float",
    "from_float_np",
    "to_f32",
    "to_f32_np",
    "to_float",
    "to_float_np",
    "fmask_to_bool",
    "enum_to_bool",
    "mask_cleanup",
    "mask_cleanup_np",
    "binary_opening",
    "binary_closing",
    "binary_dilation",
    "binary_erosion",
    "gap_fill",
    "choose_first_valid",
    "xr_geomedian",
    "int_geomedian",
    "int_geomedian_np",
    "reshape_for_geomedian",
    "geomedian_with_mads",
    "reshape_yxbt",
    "wait_for_future",
    "chunked_persist",
    "chunked_persist_da",
    "chunked_persist_ds",
    "randomize",
    "store_to_mem",
    "yxbt_sink_to_mem",
    "yxbt_sink",
    "yxt_sink",
    "da_yxt_sink",
    "da_mem_sink",
    "da_yxbt_sink",
    "is_rgb",
    "to_rgba",
    "to_rgba_np",
    "colorize",
    "xr_reproject",
    "save_cog",
    "xr_quantile",
    "pool_broadcast",
    "dask_compute_stream",
    "seq_to_bags",
)
