import pytest
from odc.io.text import mtl_parse, parse_yaml, split_and_check


def test_mtl():

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


def test_parse_yaml():
    o = parse_yaml('''
a: 3
b: foo
''')

    assert o['a'] == 3 and o['b'] == "foo"
    assert set(o) == {'a', 'b'}


def test_split_check():
    assert split_and_check("one/two/three", '/', 3) == ('one', 'two', 'three')
    assert split_and_check("one/two/three", '/', (3, 4)) == ('one', 'two', 'three')

    with pytest.raises(ValueError):
        split_and_check("a:b", ":", 3)
