""" Various file io helpers
"""
from .text import parse_yaml, read_stdin_lines, slurp_lines
from .tar import tar_doc_stream
from .path import default_base_dir, normalise_path

__all__ = (
    'parse_yaml',
    'read_stdin_lines',
    'slurp_lines',
    'tar_doc_stream',
    'default_base_dir',
    'normalise_path',
)
