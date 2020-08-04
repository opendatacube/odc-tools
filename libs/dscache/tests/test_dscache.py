from odc.dscache._dscache import parse_group_name, mk_group_name


def test_group_name():
    assert parse_group_name("g/t/33/44") == (('t', 33, 44), 'g')
    assert parse_group_name("g/33/44") == ((33, 44), 'g')

    assert mk_group_name((-1, 2), 'g') == 'g/-0001/+0002'
    assert mk_group_name(('2019--P1Y', -1, 2), 'g') == 'g/2019--P1Y/-0001/+0002'
