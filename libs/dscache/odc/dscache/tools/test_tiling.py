from uuid import UUID
from types import SimpleNamespace
import datetime
from odc.dscache.tools import group_by_nothing


def test_group_by_nothing():
    dss = [SimpleNamespace(id=UUID(int=0x1_000_000+i),
                           crs='epsg:3857',
                           center_time=datetime.datetime(2020, 1, i)) for i in range(1, 4)]

    dss.append(SimpleNamespace(id=UUID(int=0x10),
                               crs='epsg:3577',
                               center_time=datetime.datetime(2020, 1, 1)))
    print(dss)
    xx = group_by_nothing(dss)
    print(xx)
    xx = group_by_nothing(dss, datetime.timedelta(seconds=-200))
    print(xx)
    for u in xx.uuid.data:
        print(u)
