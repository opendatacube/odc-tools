""" Various file io helpers
"""

from .tar import tar_doc_stream
from .text import parse_mtl, parse_yaml, read_stdin_lines, slurp, slurp_lines
from .timer import RateEstimator

from ._version import __version__

__all__ = (
    "parse_yaml",
    "read_stdin_lines",
    "slurp",
    "slurp_lines",
    "parse_mtl",
    "tar_doc_stream",
    "RateEstimator",
    "__version__",
)
