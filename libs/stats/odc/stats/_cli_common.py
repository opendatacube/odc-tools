from typing import Tuple, List
import click

TileIdx = Tuple[str, int, int]


def parse_task(s: str) -> TileIdx:
    from odc.io.text import split_and_check
    sep = '/' if '/' in s else ','
    t, x, y = split_and_check(s, sep, 3)
    if t.startswith('x'):
        t, x, y = y, t, x
    return (t, int(x.lstrip('x')), int(y.lstrip('y')))


def parse_all_tasks(inputs: List[str], all_possible_tasks: List[TileIdx]) -> List[TileIdx]:
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
    out: List[TileIdx] = []
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


@click.group(help="Stats command line interface")
def main():
    pass
