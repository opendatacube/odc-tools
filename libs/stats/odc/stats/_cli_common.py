from typing import Tuple, List
import click

TileIdx_txy = Tuple[str, int, int]


def parse_task(s: str) -> TileIdx_txy:
    """
    Intentional copy of tasks.parse_task only for CLI parsing
    """
    from odc.io.text import split_and_check
    sep = '/' if '/' in s else ','
    t, x, y = split_and_check(s, sep, 3)
    if t.startswith('x'):
        t, x, y = y, t, x
    return (t, int(x.lstrip('x')), int(y.lstrip('y')))


def parse_all_tasks(inputs: List[str], all_possible_tasks: List[TileIdx_txy]) -> List[TileIdx_txy]:
    """
    Select a subset of all possible tasks given user input on cli.

    Every input task can be one of:
     <int>                   -- 0 based index into all_possible_tasks list

     <start>:<stop>[:<step>] -- slice of all_possible_tasks, 1:100, ::10, 1::100, -10:

     t,x,y or x,y,t triplet  -- Task index as a triplet of period,x,y
       2019--P1Y/10/-3
       2019--P1Y,10,-3
       x+10/y-3/2019--P1Y
    """
    from odc.io.text import parse_slice
    out: List[TileIdx_txy] = []
    full_set = set(all_possible_tasks)

    for s in inputs:
        if ',' in s or '/' in s:
            task = parse_task(s)
            if task not in full_set:
                raise ValueError(f"No task matches '{s}'")
            out.append(task)
        elif ':' in s:
            ii = parse_slice(s)
            out.extend(all_possible_tasks[ii])
        else:
            try:
                idx = int(s)
            except ValueError:
                raise ValueError(f"Failed to parse '{s}'") from None

            if idx < 0 or idx >= len(all_possible_tasks):
                raise ValueError(f"Task index is out of range: {idx}")
            out.append(all_possible_tasks[idx])

    return out


def setup_logging(level: int = -1):
    """
    Setup logging to print to stdout with default logging level being INFO.
    """
    import logging
    import sys

    if level < 0:
        level = logging.INFO

    logging.basicConfig(
        level=level,
        format="[%(asctime)s] {%(filename)s:%(lineno)d} %(levelname)s - %(message)s",
        stream=sys.stdout
    )


@click.group(help="Stats command line interface")
def main():
    pass
