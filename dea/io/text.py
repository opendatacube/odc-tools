try:
    from ruamel.yaml import YAML
    _YAML_C = YAML(typ='safe', pure=False)
except ImportError:
    _YAML_C = None


def _parse_yaml_yaml(s):
    import yaml
    return yaml.load(s, Loader=yaml.CSafeLoader)


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

    for l in stdin:
        l = l.strip()
        if pred(l):
            yield l


def slurp_lines(fname, *args, **kwargs):
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
