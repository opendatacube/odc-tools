from typing import Dict, Tuple
from uuid import UUID
from datetime import datetime
from dataclasses import dataclass, field

from datacube.model import GridSpec
from datacube.utils.geometry import GeoBox
from odc.index import odc_uuid


@dataclass
class OutputProduct:
    name: str
    version: str
    info: Dict[str, str]
    short_name: str
    location: str
    properties: Dict[str, str]
    measurements: Tuple[str, ...]
    gridspec: GridSpec
    period: str = '1Y'

    def region_code(self, tidx: Tuple[int, int], sep='', n=4) -> str:
        return f"x{tidx[0]:+0{n}d}{sep}y{tidx[1]:+0{n}d}"


@dataclass
class Task:
    product: OutputProduct
    tile_index: Tuple[int, int]
    geobox: GeoBox
    start_datetime: datetime
    end_datetime: datetime
    short_time: str
    input_dataset_ids: Tuple[UUID, ...]
    uuid: UUID = UUID(int=0)

    def __post_init__(self):
        if self.uuid.int == 0:
            self.uuid = odc_uuid(self.product.name,
                                 self.product.version,
                                 sources=self.input_dataset_ids,
                                 time=self.short_time,
                                 tile=self.tile_index)

    @property
    def location(self) -> str:
        """
        Product relative location for this task
        """
        return self.product.region_code(self.tile_index, '/') + '/' + self.short_time

    def _prefix(self, relative_to) -> str:
        product = self.product
        region_code = product.region_code(self.tile_index)
        file_prefix = f'{product.short_name}_{region_code}_{self.short_time}'

        if relative_to == 'dataset':
            return file_prefix
        elif relative_to == 'product':
            return self.location + '/' + file_prefix
        else:
            return product.location + '/' + self.location + '/' + file_prefix

    def paths(self, relative_to='dataset', ext='tiff') -> Dict[str, str]:
        """
        Compute dictionary mapping band name to paths.

        :param relative_to: dataset|product|absolute
        """
        prefix = self._prefix(relative_to)
        return {band: f'{prefix}_{band}.{ext}' for band in self.product.measurements}

    def metadata_path(self, relative_to='dataset', ext='yaml') -> str:
        """
        Compute path for metadata file.

        :param relative_to: dataset|product|absolute
        """
        return self._prefix(relative_to) + '.' + ext
