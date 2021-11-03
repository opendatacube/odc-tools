from typing import Tuple, Union


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
