from typing import Dict, Tuple, Sequence
from uuid import UUID
from datetime import datetime

from dataclasses import dataclass

from datacube.model import GridSpec


@dataclass
class OutputProduct:
    product_name: str
    product_version: str
    product_info: Dict[str, str]
    product_short_name: str
    algo_info: Dict[str, str]
    location: str
    properties: Dict[str, str]
    measurements: Sequence[str]


@dataclass
class Task:
    output_product: OutputProduct
    gridspec: GridSpec
    start_datetime: datetime
    end_datetime: datetime
    short_time: str
    tile_index: Tuple[int, int]
    input_dataset_ids: Sequence[UUID]
