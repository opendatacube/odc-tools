from sys import stdin

from typing import Tuple, Union, Dict, Any, Iterator, List, Optional
from pathlib import Path

PathLike = Union[str, Path]
RawDoc = Union[str, bytes]

try:
    from ruamel.yaml import YAML

    _YAML_C = YAML(typ="safe", pure=False)
except ImportError:
    _YAML_C = None


def _parse_yaml_yaml(s: str) -> Dict[str, Any]:
    # pylint: disable=import-outside-toplevel
    import yaml

    return yaml.load(s, Loader=getattr(yaml, "CSafeLoader", yaml.SafeLoader))


def _parse_yaml_ruamel(s: str) -> Dict[str, Any]:
    return _YAML_C.load(s)


parse_yaml = _parse_yaml_yaml if _YAML_C is None else _parse_yaml_ruamel


def _guess_is_file(s: str):
    try:
        return Path(s).exists()
    except IOError:
        return False


def parse_yaml_file_or_inline(s: str) -> Dict[str, Any]:
    """
    Accept on input either a path to yaml file or yaml text, return parsed yaml document.
    """
    if _guess_is_file(s):
        txt = slurp(s, binary=False)
        assert isinstance(txt, str)
    else:
        txt = s

    result = parse_yaml(txt)
    if isinstance(result, str):
        raise IOError(f"No such file: {s}")

    return result


def read_stdin_lines(skip_empty: bool = False) -> Iterator[str]:
    """Read lines from stdin.

    Returns iterator of lines with any whitespace trimmed.

    skip_empty - when True whitespace only lines will be omitted
    """
    pred = {True: lambda s: len(s) > 0, False: lambda s: True}[skip_empty]

    for line in stdin:
        line = line.strip()
        if pred(line):
            yield line


def slurp(fname: PathLike, binary: bool = False) -> RawDoc:
    """fname -> str|bytes.

    binary=True -- read bytes not text
    """
    mode = "rb" if binary else "rt"

    with open(fname, mode) as f:
        return f.read()


def slurp_lines(fname: str, *args, **kwargs) -> List[str]:
    """file path -> [lines]"""
    if len(args) > 0 or len(kwargs) > 0:
        fname = fname.format(*args, **kwargs)

    with open(fname, "rt") as f:
        return [s.rstrip() for s in f.readlines()]


def read_int(path: PathLike, default=None, base=10) -> Optional[int]:
    """
    Read single integer from a text file.

    Useful for things like parsing content of /sys/ or /proc.
    """
    try:
        return int(slurp(path), base)
    except (FileNotFoundError, ValueError):
        return default


def parse_mtl(txt: str) -> Dict[str, Any]:
    def parse_value(s):
        if len(s) == 0:
            return s
        if s[0] == '"':
            return s.strip('"')

        for parser in [int, float]:
            try:
                return parser(s)
            except ValueError:
                pass
        return s

    def tokenize(lines):
        if isinstance(lines, str):
            lines = lines.splitlines()

        for lineno, s in enumerate(lines):
            if len(s) == 0:
                continue

            i = s.find("=")
            if i < 0:
                if s.strip() == "END":
                    break
                raise ValueError("Can not parse:[%d]: %s" % (lineno, s))

            k = s[:i].strip()
            v = s[i + 1 :].strip()
            yield (k, v)

    tree: Dict[str, Any] = {}
    node, name = tree, None
    nodes = []

    for k, v in tokenize(txt):
        if k == "GROUP":
            nodes.append((node, name))
            parent, node, name = node, {}, v
            if name in parent:
                raise ValueError("Repeated key: %s" % name)
            parent[name] = node
        elif k == "END_GROUP":
            if len(nodes) == 0:
                raise ValueError("Bad END_GROUP: too many")
            if name != v:
                raise ValueError("Bad END_GROUP: bad name")
            node, name = nodes.pop()
        else:
            if k in node:
                raise ValueError("Repeated key: %s" % k)
            node[k] = parse_value(v)

    return tree


def split_and_check(
    s: str, separator: str, n: Union[int, Tuple[int, ...]]
) -> Tuple[str, ...]:
    """Turn string into tuple, checking that there are exactly as many parts as expected.
    :param s: String to parse
    :param separator: Separator character
    :param n: Expected number of parts, can be a single integer value or several,
              example `(2, 3)` accepts 2 or 3 parts.
    """
    if isinstance(n, int):
        n = (n,)

    parts = s.split(separator)
    if len(parts) not in n:
        raise ValueError('Failed to parse "{}"'.format(s))
    return tuple(parts)


def parse_range_int(s: str, separator: str = ":") -> Tuple[int, int]:
    """Parse str(<int>:<int>) -> (int, int)"""
    try:
        _in, _out = (int(x) for x in split_and_check(s, separator, 2))
    except ValueError:
        raise ValueError(
            'Expect <int>{}<int> syntax, got "{}"'.format(separator, s)
        ) from None

    return (_in, _out)


def parse_range2d_int(s: str) -> Tuple[Tuple[int, int], Tuple[int, int]]:
    """Parse string like "0:3,4:5" -> ((0,3), (4,5))"""
    try:
        a, b = (parse_range_int(p, ":") for p in split_and_check(s, ",", 2))
    except ValueError:
        raise ValueError(
            'Expect <int>:<int>,<int>:<int> syntax, got "{}"'.format(s)
        ) from None
    return a, b


# pylint: disable=import-outside-toplevel,inconsistent-return-statements
def click_range2d(ctx, param, value):
    """
    @click.option('--range', callback=click_range2d)
    """
    import click

    if value is not None:
        try:
            return parse_range2d_int(value)
        except ValueError as e:
            raise click.ClickException(str(e)) from None


def parse_slice(s: str) -> slice:
    """
    Parse slice syntax in the form start:stop[:step]
    Examples "::4", "2:5", "2::10", "3:100:5"
    """

    def parse(part: str) -> Optional[int]:
        if part == "":
            return None
        return int(part)

    try:
        parts = [parse(p) for p in split_and_check(s, ":", (2, 3))]
    except ValueError:
        raise ValueError(f'Expect <start>:<stop>[:<step>] syntax, got "{s}"') from None

    return slice(*parts)


# pylint: disable=import-outside-toplevel,inconsistent-return-statements
def click_slice(ctx, param, value):
    """
    @click.option('--slice', callback=click_slice)
    Examples "::4", "2:5", "2::10", "3:100:5"
    """
    import click

    if value is not None:
        try:
            return parse_slice(value)
        except ValueError as e:
            raise click.ClickException(str(e)) from None
