""" Various file io helpers
"""
from ._version import __version__
from .text import (
    parse_yaml,
    read_stdin_lines,
    parse_mtl,
    slurp_lines,
    slurp,
)
from .tar import tar_doc_stream
from .timer import RateEstimator


__all__ = (
    "parse_yaml",
    "read_stdin_lines",
    "slurp",
    "slurp_lines",
    "parse_mtl",
    "tar_doc_stream",
    "RateEstimator",
)
