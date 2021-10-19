import pytest
from odc.stats._text import read_int, parse_slice, parse_range2d_int, \
                            parse_yaml, parse_yaml_file_or_inline, \
                            split_and_check


def test_read_int():
    assert read_int("/not_a_real_file/already", default="bleagh") == "bleagh"


def test_parse_yaml():
    o = parse_yaml(
        """
a: 3
b: foo
"""
    )

    assert o["a"] == 3 and o["b"] == "foo"
    assert set(o) == {"a", "b"}
    o = parse_yaml_file_or_inline(
        """
a: 3
b: foo
"""
    )

    assert o["a"] == 3 and o["b"] == "foo"
    assert set(o) == {"a", "b"}


def test_split_check():
    assert split_and_check("one/two/three", "/", 3) == ("one", "two", "three")
    assert split_and_check("one/two/three", "/", (3, 4)) == ("one", "two", "three")

    with pytest.raises(ValueError):
        split_and_check("a:b", ":", 3)


def test_parse_slice():
    from numpy import s_

    assert parse_slice("::2") == s_[::2]
    assert parse_slice("1:") == s_[1:]
    assert parse_slice("1:4") == s_[1:4]
    assert parse_slice("1:4:2") == s_[1:4:2]
    assert parse_slice("1::2") == s_[1::2]

def test_parse_2d_range():
    assert parse_range2d_int("1:2,3:4") == ((1,2),(3,4))
    with pytest.raises(ValueError):
        parse_range2d_int("1,2:3,4")
