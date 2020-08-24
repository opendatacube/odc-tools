from typing import Tuple
import click


def parse_task(s: str) -> Tuple[str, int, int]:
    from odc.io.text import split_and_check
    sep = '/' if '/' in s else ','
    t, x, y = split_and_check(s, sep, 3)
    if t.startswith('x'):
        t, x, y = y, t, x
    return (t, int(x.lstrip('x')), int(y.lstrip('y')))


@click.group(help="Stats command line interface")
def main():
    pass
