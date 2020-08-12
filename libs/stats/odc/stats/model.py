from typing import Dict, Tuple, Any, Optional, Union
from copy import deepcopy
from datetime import datetime
from dataclasses import dataclass, field
from uuid import UUID
import pandas as pd

from datacube.model import GridSpec, Dataset
from datacube.utils.geometry import GeoBox
from datacube.utils.dates import normalise_dt
from odc.index import odc_uuid
from odc.io.text import split_and_check


default_href_prefix = 'https://collections.dea.ga.gov.au/product'


def format_datetime(dt: datetime,
                    with_tz=True,
                    timespec='microseconds') -> str:
    dt = normalise_dt(dt)
    dt = dt.isoformat(timespec=timespec)
    if with_tz:
        dt = dt + 'Z'
    return dt


@dataclass
class DateTimeRange:
    __slots__ = ('start', 'end', 'freq')

    def __init__(self, start: Union[str, datetime],
                 freq: Optional[str] = None):
        """

        DateTimeRange('2019-03--P3M')
        DateTimeRange('2019-03', '3M')
        DateTimeRange(datetime(2019, 3, 1), '3M')

        """
        if freq is None:
            assert isinstance(start, str)
            start, freq = split_and_check(start, '--P', 2)

        freq = freq.upper()
        # Pandas period snaps to frequency resolution, we need to undo that by re-adding the snapping delta
        t0 = pd.Timestamp(start)
        period = pd.Period(t0, freq=freq)
        dt = t0 - period.start_time

        self.freq: str = freq
        self.start: datetime = normalise_dt(t0.to_pydatetime(warn=False))
        self.end: datetime = normalise_dt((period.end_time + dt).to_pydatetime(warn=False))

    def __str__(self):
        return self.short

    def __repr__(self):
        return f'DateTimeRange({repr(self.start)}, {repr(self.freq)})'

    def to_stac(self) -> Dict[str, str]:
        """
        Return dictionary of values to go into STAC's `properties:` section.
        """
        start = format_datetime(self.start)
        end = format_datetime(self.end)

        return {'datetime': start,
                'dtr:start_datetime': start,
                'dtr:end_datetime': end}

    @property
    def short(self) -> str:
        """
        Short string representation of the time period.

        Examples: 2019--1Y, 2020-01--3M, 2013-03-21--10D
        """
        freq = self.freq
        dt = self.start
        if freq.endswith('Y') and dt.month == 1 and dt.day == 1:
            return f'{dt.year}--P{freq}'
        elif freq.endswith('M') and dt.day == 1:
            return f'{dt.year}-{dt.month:02d}--P{freq}'
        else:
            return f'{dt.year}-{dt.month:02d}-{dt.day:02d}--P{freq}'


@dataclass
class OutputProduct:
    name: str
    version: str
    short_name: str
    location: str
    properties: Dict[str, str]
    measurements: Tuple[str, ...]
    gridspec: GridSpec
    href: str = ''
    freq: str = '1Y'

    def __post_init__(self):
        if self.href == '':
            self.href = f'{default_href_prefix}/{self.name}'

    def region_code(self, tidx: Tuple[int, int], sep='', n=4) -> str:
        """
        Render tile index into a string.
        """
        return f"x{tidx[0]:+0{n}d}{sep}y{tidx[1]:+0{n}d}"


@dataclass
class Task:
    product: OutputProduct
    tile_index: Tuple[int, int]
    geobox: GeoBox
    time_range: DateTimeRange
    datasets: Tuple[Dataset, ...] = field(repr=False)
    uuid: UUID = UUID(int=0)
    short_time: str = field(init=False, repr=False)

    def __post_init__(self):
        self.short_time = self.time_range.short

        if self.uuid.int == 0:
            self.uuid = odc_uuid(self.product.name,
                                 self.product.version,
                                 sources=self._lineage(),
                                 time=self.short_time,
                                 tile=self.tile_index)

    @property
    def location(self) -> str:
        """
        Product relative location for this task
        """
        return self.product.region_code(self.tile_index, '/') + '/' + self.short_time

    def _lineage(self) -> Tuple[UUID, ...]:
        return tuple(ds.id for ds in self.datasets)

    def _prefix(self, relative_to: str = 'dataset') -> str:
        product = self.product
        region_code = product.region_code(self.tile_index)
        file_prefix = f'{product.short_name}_{region_code}_{self.short_time}'

        if relative_to == 'dataset':
            return file_prefix
        elif relative_to == 'product':
            return self.location + '/' + file_prefix
        else:
            return product.location + '/' + self.location + '/' + file_prefix

    def paths(self, relative_to: str = 'dataset', ext: str = 'tiff') -> Dict[str, str]:
        """
        Compute dictionary mapping band name to paths.

        :param relative_to: dataset|product|absolute
        """
        prefix = self._prefix(relative_to)
        return {band: f'{prefix}_{band}.{ext}' for band in self.product.measurements}

    def metadata_path(self, relative_to: str = 'dataset', ext: str = 'yaml') -> str:
        """
        Compute path for metadata file.

        :param relative_to: dataset|product|absolute
        """
        return self._prefix(relative_to) + '.' + ext

    def render_metadata(self, ext: str = 'tiff',
                        processing_dt: Optional[datetime] = None) -> Dict[str, Any]:
        """
        Put together EO3 metadata document for the output of this task.
        """
        if processing_dt is None:
            processing_dt = datetime.utcnow()

        product = self.product
        geobox = self.geobox
        region_code = product.region_code(self.tile_index)
        properties = deepcopy(product.properties)

        properties.update(self.time_range.to_stac())
        properties['odc:processing_datetime'] = format_datetime(processing_dt, timespec='seconds')
        properties['odc:region_code'] = region_code

        measurements = {band: {'path': path}
                        for band, path in self.paths(ext=ext).items()}

        inputs = list(map(str, self._lineage()))

        return {
            '$schema': 'https://schemas.opendatacube.org/dataset',
            'id': str(self.uuid),
            'product': dict(name=product.name,
                            href=product.href),
            'location': self.metadata_path('absolute', ext='yaml'),

            'crs': str(geobox.crs),
            'grids': {'default': dict(shape=list(geobox.shape),
                                      transform=list(geobox.transform))},

            'measurements': measurements,
            'properties': properties,
            'lineage': dict(inputs=inputs),
        }
