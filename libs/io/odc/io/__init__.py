""" Various file io helpers
"""
from .text import (
    parse_yaml,
    read_stdin_lines,
    parse_mtl,
    slurp_lines,
    slurp,
)
from .tar import tar_doc_stream
from .path import default_base_dir, normalise_path
from .timer import RateEstimator


__all__ = (
    "parse_yaml",
    "read_stdin_lines",
    "slurp",
    "slurp_lines",
    "parse_mtl",
    "tar_doc_stream",
    "default_base_dir",
    "normalise_path",
    "RateEstimator",
)
