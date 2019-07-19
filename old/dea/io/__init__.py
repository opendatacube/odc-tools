""" Various file io helpers
"""
from odc.io.text import parse_yaml, read_stdin_lines, slurp_lines
from odc.io.tar import tar_doc_stream
from odc.io.path import default_base_dir, normalise_path

__all__ = (
    'parse_yaml',
    'read_stdin_lines',
    'slurp_lines',
    'tar_doc_stream',
    'default_base_dir',
    'normalise_path',
)
