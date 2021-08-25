""" Various Algorithmic Helpers

"""

from ._version import __version__
from ._numexpr import apply_numexpr, safe_div

from ._masking import (
    keep_good_np,
    keep_good_only,
    erase_bad,
    from_float,
    from_float_np,
    to_f32,
    to_f32_np,
    to_float,
    to_float_np,
    fmask_to_bool,
    enum_to_bool,
    gap_fill,
    choose_first_valid,
    mask_cleanup,
    mask_cleanup_np,
    binary_opening,
    binary_closing,
    binary_dilation,
    binary_erosion,
)

from ._geomedian import (
    xr_geomedian,
    reshape_for_geomedian,
    geomedian_with_mads,
    int_geomedian,
    int_geomedian_np,
)

from ._dask import (
    chunked_persist,
    chunked_persist_ds,
    chunked_persist_da,
    randomize,
    reshape_yxbt,
    wait_for_future,
)

from ._memsink import (
    store_to_mem,
    yxbt_sink_to_mem,
    da_yxbt_sink,
    yxbt_sink,
    da_mem_sink,
)

from ._rgba import (
    is_rgb,
    to_rgba,
    to_rgba_np,
    colorize,
)

from ._warp import xr_reproject

from ._tiff import save_cog

from ._broadcast import (
    pool_broadcast,
)

from ._dask_stream import (
    dask_compute_stream,
    seq_to_bags,
)


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
    "da_mem_sink",
    "da_yxbt_sink",
    "is_rgb",
    "to_rgba",
    "to_rgba_np",
    "colorize",
    "xr_reproject",
    "save_cog",
    "pool_broadcast",
    "dask_compute_stream",
    "seq_to_bags",
)
