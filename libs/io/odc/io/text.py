from typing import Tuple

try:
    from ruamel.yaml import YAML
    _YAML_C = YAML(typ='safe', pure=False)
except ImportError:
    _YAML_C = None


def _parse_yaml_yaml(s):
    import yaml
    return yaml.load(s, Loader=getattr(yaml, 'CSafeLoader', yaml.SafeLoader))


def _parse_yaml_ruamel(s):
    return _YAML_C.load(s)


parse_yaml = _parse_yaml_yaml if _YAML_C is None else _parse_yaml_ruamel


def read_stdin_lines(skip_empty=False):
    """ Read lines from stdin.

    Returns iterator of lines with any whitespace trimmed.

    skip_empty - when True whitespace only lines will be omitted
    """
    from sys import stdin

    pred = {True: lambda s: len(s) > 0,
            False: lambda s: True}[skip_empty]

    for line in stdin:
        line = line.strip()
        if pred(line):
            yield line


def slurp(fname, binary=False):
    """ fname -> str|bytes.

    binary=True -- read bytes not text
    """
    mode = 'rb' if binary else 'rt'

    with open(fname, mode) as f:
        return f.read()


def slurp_lines(fname, *args, **kwargs):
    """ file path -> [lines]
    """
    if len(args) > 0 or len(kwargs) > 0:
        fname = fname.format(*args, **kwargs)

    with open(fname, 'rt') as f:
        return [s.rstrip() for s in f.readlines()]


def test_parse_yaml():
    o = parse_yaml('''
a: 3
b: foo
''')

    assert o['a'] == 3 and o['b'] == "foo"
    assert set(o) == {'a', 'b'}


def mtl_parse(txt):
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

            i = s.find('=')
            if i < 0:
                if s.strip() == 'END':
                    break
                raise ValueError('Can not parse:[%d]: %s' % (lineno, s))

            k = s[:i].strip()
            v = s[i+1:].strip()
            yield (k, v)

    tree = {}
    node, name = tree, None
    nodes = []

    for k, v in tokenize(txt):
        if k == 'GROUP':
            nodes.append((node, name))
            parent, node, name = node, {}, v
            if name in parent:
                raise ValueError('Repeated key: %s' % name)
            parent[name] = node
        elif k == 'END_GROUP':
            if len(nodes) == 0:
                raise ValueError('Bad END_GROUP: too many')
            if name != v:
                raise ValueError('Bad END_GROUP: bad name')
            node, name = nodes.pop()
        else:
            if k in node:
                raise ValueError('Repeated key: %s' % k)
            node[k] = parse_value(v)

    return tree


def test_mtl():
    import pytest

    txt = '''
    GROUP = a
      a_int = 10
      GROUP = b
         b_string = "string with spaces"
         GROUP = c
           c_float = 1.34
           c_int = 3
         END_GROUP = c
         b_more = 2e-4
      END_GROUP = b
      a_date = 2018-09-23
    END_GROUP = a
    END
    '''

    expect = {'a': {'a_int': 10,
                    'a_date': "2018-09-23",
                    'b': {'b_string': "string with spaces",
                          'b_more': 2e-4,
                          'c': {'c_float': 1.34,
                                'c_int': 3}}}}

    doc = mtl_parse(txt)
    assert doc == expect

    with pytest.raises(ValueError):
        mtl_parse("""
        GROUP = a
        END_GROUP = b
        """)

    with pytest.raises(ValueError):
        mtl_parse("""
        GROUP = a
        GROUP = b
        END_GROUP = b
        END_GROUP = a
        END_GROUP = a
        """)

    # test duplicate keys: values
    with pytest.raises(ValueError):
        mtl_parse("""
        a = 10
        a = 3
        """)

    # test duplicate keys: values/subtrees
    with pytest.raises(ValueError):
        mtl_parse("""
        GROUP = a
        b = 10
          GROUP = b
          END_GROUP = b
        END_GROUP = a
        """)

    assert mtl_parse("") == {}
    assert mtl_parse("END") == {}


def split_and_check(s, separator, n):
    """ Turn string into tuple, checking that there are exactly as many parts as expected.
    """
    parts = s.split(separator)
    if len(parts) != n:
        raise ValueError('Failed to parse "{}"'.format(s))
    return tuple(parts)


def parse_range_int(s: str, separator: str = ':') -> Tuple[int, int]:
    """ Parse str(<int>:<int>) -> (int, int)
    """
    try:
        _in, _out = (int(x) for x in split_and_check(s, separator, 2))
    except ValueError:
        raise ValueError('Expect <int>{}<int> syntax, got "{}"'.format(separator, s)) from None

    return (_in, _out)


def parse_range2d_int(s: str) -> Tuple[Tuple[int, int], Tuple[int, int]]:
    """ Parse string like "0:3,4:5" -> ((0,3), (4,5))
    """
    try:
        a, b = (parse_range_int(p, ':') for p in split_and_check(s, ',', 2))
    except ValueError:
        raise ValueError('Expect <int>:<int>,<int>:<int> syntax, got "{}"'.format(s)) from None
    return a, b


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
