""" Various file io helpers
"""


def read_stdin_lines(skip_empty=False):
    """ Read lines from stdin.

    Returns iterator of lines with any whitespace trimmed.

    skip_empty - when True whitespace only lines will be omitted
    """
    from sys import stdin

    pred = {True: lambda s: len(s) > 0,
            False: lambda s: True}[skip_empty]

    for l in stdin:
        l = l.strip()
        if pred(l):
            yield l


def slurp_lines(fname, *args, **kwargs):
    if len(args) > 0 or len(kwargs) > 0:
        fname = fname.format(*args, **kwargs)

    with open(fname, 'rt') as f:
        return [s.rstrip() for s in f.readlines()]
