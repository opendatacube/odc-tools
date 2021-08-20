import math
from pathlib import Path
from abc import ABC, abstractmethod
from copy import deepcopy
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Optional, Tuple, Union, List, Mapping
from uuid import UUID
import json
import pandas as pd
from pyproj.transformer import transform
import xarray as xr
from toolz import dicttoolz
from rasterio.crs import CRS

from datacube.model import Dataset
from datacube.utils.dates import normalise_dt
from datacube.utils.geometry import GeoBox
from datacube.testutils.io import native_geobox
from odc.index import odc_uuid
from odc.io.text import split_and_check

from eodatasets3.assemble import DatasetAssembler, serialise
from eodatasets3.images import GridSpec
from eodatasets3.model import DatasetDoc, ProductDoc, GridDoc
from eodatasets3.properties import StacPropertyView
from eodatasets3.verify import PackageChecksum

TileIdx_xy = Tuple[int, int]
TileIdx_txy = Tuple[str, int, int]
TileIdx = Union[TileIdx_txy, TileIdx_xy]

default_href_prefix = "https://collections.dea.ga.gov.au/product"
EXT_TIFF = "tif"  # because "consistency"


def format_datetime(dt: datetime, with_tz=True, timespec="microseconds") -> str:
    dt = normalise_dt(dt)
    dt = dt.isoformat(timespec=timespec)
    if with_tz:
        dt = dt + "Z"
    return dt


@dataclass
class DateTimeRange:

    __slots__ = ("start", "end", "freq")

    def __init__(self, start: Union[str, datetime], freq: Optional[str] = None):
        """

        DateTimeRange('2019-03--P3M')
        DateTimeRange('2019-03', '3M')
        DateTimeRange(datetime(2019, 3, 1), '3M')

        """

        if freq is None:
            assert isinstance(start, str)
            start, freq = split_and_check(start, "--P", 2)
        freq = freq.upper().lstrip("P")
        # Pandas period snaps to frequency resolution, we need to undo that by re-adding the snapping delta
        t0 = pd.Timestamp(start)
        period = pd.Period(t0, freq=freq)
        dt = t0 - period.start_time

        self.freq: str = freq

        self.start: datetime = normalise_dt(t0.to_pydatetime(warn=False))
        self.end: datetime = normalise_dt(
            (period.end_time + dt).to_pydatetime(warn=False)
        )

    @staticmethod
    def year(year: int) -> "DateTimeRange":
        """
        Construct ``DateTimeRange`` covering one whole year.
        """
        return DateTimeRange(datetime(year, 1, 1), "1Y")

    def __str__(self):
        return self.short

    def __repr__(self):
        return f"DateTimeRange({repr(self.start)}, {repr(self.freq)})"

    def dc_query(
        self, pad: Optional[Union[timedelta, float, int]] = None
    ) -> Dict[str, Any]:
        """
        Transform to form understood by datacube

        :param pad: optionally pad the region by X days, or timedelta

        Example: ``dc.load(..., **p.dc_query(pad=0.5))``
        """
        if pad is None:
            return {"time": (self.start, self.end)}

        if isinstance(pad, (int, float)):
            pad = timedelta(days=pad)

        return {"time": (self.start - pad, self.end + pad)}

    @property
    def short(self) -> str:
        """
        Short string representation of the time period.

        Examples: 2019--P1Y, 2020-01--P3M, 2013-03-21--P10D
        """
        freq = self.freq
        dt = self.start
        if freq.endswith("Y") and dt.month == 1 and dt.day == 1:
            return f"{dt.year}--P{freq}"
        elif freq.endswith("M") and dt.day == 1:
            return f"{dt.year}-{dt.month:02d}--P{freq}"
        else:
            return f"{dt.year}-{dt.month:02d}-{dt.day:02d}--P{freq}"

    def __contains__(self, t: datetime) -> bool:
        return self.start <= t <= self.end

    def __lt__(self, t: datetime) -> bool:
        return self.start < t

    def __gt__(self, t: datetime) -> bool:
        return self.end > t

    def to_pandas(self) -> pd.Period:
        """
        Convert to pandas Period object
        """
        return pd.Period(self.start, self.freq)

    def __add__(self, v: int) -> "DateTimeRange":
        p = self.to_pandas() + v
        return DateTimeRange(p.start_time.to_pydatetime(warn=False), self.freq)

    def __sub__(self, v: int) -> "DateTimeRange":
        p = self.to_pandas() - v
        return DateTimeRange(p.start_time.to_pydatetime(warn=False), self.freq)


@dataclass
class OutputProduct:
    name: str
    version: str
    short_name: str
    location: str
    properties: Dict[str, str]
    measurements: Tuple[str, ...]
    href: str = ""
    region_code_format: str = "x{x:02d}y{y:02d}"
    cfg: Any = None
    naming_conventions_values: str = "dea_c3"
    explorer_path: str = "https://explorer.dea.ga.gov.au/"
    inherit_skip_properties: Optional[List[str]] = None
    preview_image: Optional[List[Any]] = None
    preview_image_singleband: Optional[List[Any]] = None
    classifier: str = "level3"
    maturity: str = "final"
    collection_number: int = 3
    nodata: int = -999

    def __post_init__(self):
        if self.href == "":
            self.href = f"{default_href_prefix}/{self.name}"

    def region_code(self, tidx: TileIdx_xy) -> str:
        """
        Render tile index into a string.
        """
        x, y = tidx
        return self.region_code_format.format(x=x, y=y)

    @staticmethod
    def dummy(
        measurements: Tuple[str, ...] = ("red", "green", "blue")
    ) -> "OutputProduct":
        version = "0.0.0"
        name = "dummy"
        short_name = "dmy"
        return OutputProduct(
            name=name,
            version=version,
            short_name=short_name,
            location=f"s3://dummy-bucket/{name}/{version}",
            properties={"odc:file_format": "GeoTIFF"},
            measurements=measurements,
        )


class WorkTokenInterface(ABC):
    @staticmethod
    def now():
        """
        Implementations should use this method internally
        """
        return datetime.utcnow()

    @property
    @abstractmethod
    def start_time(self) -> datetime:
        """
        Should return timestamp when task "started"
        """

    @property
    @abstractmethod
    def deadline(self) -> datetime:
        """
        Should return timestamp by which work is to be completed
        """

    @property
    @abstractmethod
    def done(self):
        """
        Called when work is completed successfully
        """

    @abstractmethod
    def cancel(self):
        """
        Called when work is terminated for whatever reason without successful result
        """
        pass

    @abstractmethod
    def extend(self, seconds: int) -> bool:
        """
        Called to extend work deadline
        """
        pass

    @property
    def active_seconds(self) -> float:
        """
        :returns: Number of seconds this Token has been active for
        """
        return (self.now() - self.start_time).total_seconds()

    def extend_if_needed(self, seconds, buffer_seconds: int = 30) -> bool:
        """
        Call ``.extend(seconds)`` only if deadline is within ``buffer_seconds`` from now

        :returns: True if deadline is still too far in the future
        :returns: Result of ``.extend(seconds)`` if deadline is close enough
        """
        t_now = self.now()
        if t_now + timedelta(seconds=buffer_seconds) > self.deadline:
            return self.extend(seconds)
        return True


@dataclass
class Task:
    product: OutputProduct
    tile_index: TileIdx_xy
    geobox: GeoBox
    time_range: DateTimeRange
    datasets: Tuple[Dataset, ...] = field(repr=False)
    uuid: UUID = UUID(int=0)
    short_time: str = field(init=False, repr=False)
    source: Optional[WorkTokenInterface] = field(init=True, repr=False, default=None)

    def __post_init__(self):
        self.short_time = self.time_range.short

        if self.uuid.int == 0:
            self.uuid = odc_uuid(
                self.product.name,
                self.product.version,
                sources=self._lineage(),
                time=self.short_time,
                tile=self.tile_index,
            )

    @property
    def location(self) -> str:
        """
        Product relative location for this task
        """
        rc = self.product.region_code(self.tile_index)
        mid = len(rc) // 2
        p1, p2 = rc[:mid], rc[mid:]
        return "/".join([p1, p2, self.short_time])

    def _lineage(self) -> Tuple[UUID, ...]:
        ds, *_ = self.datasets

        # TODO: replace this and test
        # if 'fused' in ds.metadata._doc['properties'].keys():
        if 'fused' in ds.type.name:
            lineage = tuple(set(
                x for ds in self.datasets for y in ds.metadata.sources.values() for x in y.values()
            ))
        else:
            lineage = tuple(ds.id for ds in self.datasets)

        return lineage

    def _prefix(self, relative_to: str = "dataset") -> str:
        product = self.product
        region_code = product.region_code(self.tile_index)
        file_prefix = f"{product.short_name}_{region_code}_{self.short_time}"

        if relative_to == "dataset":
            return file_prefix
        elif relative_to == "product":
            return self.location + "/" + file_prefix
        else:
            return product.location + "/" + self.location + "/" + file_prefix

    def paths(
        self, relative_to: str = "dataset", ext: str = EXT_TIFF
    ) -> Dict[str, str]:
        """
        Compute dictionary mapping band name to paths.

        :param relative_to: dataset|product|absolute
        """
        prefix = self._prefix(relative_to)
        return {band: f"{prefix}_{band}.{ext}" for band in self.product.measurements}

    def metadata_path(self, relative_to: str = "dataset", ext: str = "yaml") -> str:
        """
        Compute path for metadata file.

        :param relative_to: dataset|product|absolute
        """
        return self._prefix(relative_to) + "." + ext

    def aux_path(self, name: str, relative_to: str = "dataset", ext: str = EXT_TIFF):
        """
        Compute path for some auxilary file.

        :param relative_to: dataset|product|absolute
        :param name: "band"
        :param ext: File extension, defaults to tif
        """
        prefix = self._prefix(relative_to)
        return f"{prefix}_{name}.{ext}"

    def render_metadata(
        self, ext: str = EXT_TIFF, output_dataset: xr.Dataset = None, processing_dt: Optional[datetime] = None
    ) -> DatasetAssembler:
        """
        Put together metadata document for the output of this task. It needs the source_dataset to inherit
        several properties and lineages. It also needs the output_dataset to get the measurement information.
        """
        dataset_assembler = DatasetAssembler(naming_conventions=self.product.naming_conventions_values,
                                             dataset_location=Path(self.product.explorer_path),
                                             allow_absolute_paths=True)

        platforms = [] # platforms are the concat value in stats

        for dataset in self.datasets:
            source_datasetdoc = serialise.from_doc(dataset.metadata_doc, skip_validation=True)
            dataset_assembler.add_source_dataset(source_datasetdoc,
                                                 classifier=self.product.classifier,
                                                 auto_inherit_properties=True, # it will grab all useful input dataset preperties
                                                 inherit_geometry=True,
                                                 inherit_skip_properties=self.product.inherit_skip_properties)
            if 'eo:platform' in source_datasetdoc.properties:
                platforms.append(source_datasetdoc.properties['eo:platform'])

        if len(platforms) > 0:
            dataset_assembler.platform = ','.join(sorted(set(platforms)))

        # inherit properties from cfg
        for product_property_name, product_property_value in self.product.properties.items():
            dataset_assembler.properties[product_property_name] = product_property_value

        dataset_assembler.product_name = self.product.name
        dataset_assembler.dataset_version = self.product.version
        dataset_assembler.region_code = self.product.region_code(self.tile_index)

        if processing_dt is None:
            processing_dt = datetime.utcnow()
        dataset_assembler.processed = processing_dt

        dataset_assembler.maturity = self.product.maturity
        dataset_assembler.collection_number = self.product.collection_number

        
        for band, path in self.paths(ext=ext).items():
            pixels = output_dataset[band].values.reshape([self.geobox.shape[0], self.geobox.shape[1]]) if band in output_dataset else None
            # when we pass grid, the eodatasets will not load file from path
            dataset_assembler.note_measurement(band,
                                                path,
                                                pixels=pixels,
                                                grid=GridSpec(shape=self.geobox.shape,
                                                                transform=self.geobox.transform,
                                                                crs=CRS.from_epsg(self.geobox.crs.to_epsg())),
                                                nodata=self.product.nodata)

        return dataset_assembler


class StatsPluginInterface(ABC):
    NAME = "*unset*"
    SHORT_NAME = ""
    VERSION = "0.0.0"
    PRODUCT_FAMILY = "statistics"

    @property
    @abstractmethod
    def measurements(self) -> Tuple[str, ...]:
        pass

    @abstractmethod
    def input_data(self, task: Task) -> xr.Dataset:
        pass

    @abstractmethod
    def reduce(self, xx: xr.Dataset) -> xr.Dataset:
        pass

    def rgba(self, xx: xr.Dataset) -> Optional[xr.DataArray]:
        """
        Given result of ``.reduce(..)`` optionally produce RGBA preview image
        """
        return None

    def product(
        self,
        location: str,
        name: Optional[str] = None,
        short_name: Optional[str] = None,
        version: Optional[str] = None,
        product_family: Optional[str] = None,
        collections_site: str = "collections.dea.ga.gov.au",
        producer: str = "ga.gov.au",
        properties: Dict[str, Any] = dict(),
        region_code_format: str = "x{x:02d}y{y:02d}",
        naming_conventions_values: str = "dea_c3",
        explorer_path: str = "https://explorer.dea.ga.gov.au",
        inherit_skip_properties: Optional[List[str]] = None,
        preview_image: Optional[List[Any]] = None,
        preview_image_singleband: Optional[List[Any]] = None,
        classifier: str = "level3",
        maturity: str = "final",
        collection_number: int = 3,
        nodata: int = -999,
    ) -> OutputProduct:
        """
        :param location: Output location string or template, example ``s3://bucket/{product}/{version}``
        :param name: Override for product name
        :param short_name: Override for product short_name
        :param version: Override for version
        :param product_family: Override for odc:product_family
        :param collections_site: href=f"https://{collections_site}/product/{name}"
        :param producer: Producer ``ga.gov.au``
        :param region_code_format: Change region code formatting, default ``"x{x:02d}y{y:02d}"``
        :param naming_conventions_values: default ``dea_c3``
        :param explorer_path: default ``https://explorer.dea.ga.gov.au``
        :param inherit_skip_properties: block properties from source datasets.
        :param preview_image: three measurement display as a thumbnail setting. Three values map to green, red and blue.
        :param preview_image_singleband: each measurment display as a thumbnail setting.
        :param classifier: default ``level3``
        :param maturity: default ``final``
        :param collection_number: default ``3``
        :param nodata: default ``-999``
        """
        if name is None:
            name = self.NAME
        if short_name is None:
            short_name = self.SHORT_NAME
            if len(short_name) == 0:
                short_name = name
        if version is None:
            version = self.VERSION
        if product_family is None:
            product_family = self.PRODUCT_FAMILY

        if "{" in location and "}" in location:
            version_dashed = version.replace(".", "-")
            location = location.format(
                name=name,
                product=name,
                short_name=short_name,
                version=version_dashed,
                version_dashed=version_dashed,
                version_raw=version,
            )

        # remove trailing / if present
        location = location.rstrip("/")

        return OutputProduct(
            name=name,
            version=version,
            short_name=short_name,
            location=location,
            properties={
                "odc:file_format": "GeoTIFF",
                "odc:product_family": product_family,
                "odc:producer": producer,
                **properties,
            },
            measurements=self.measurements,
            href=f"https://{collections_site}/product/{name}",
            region_code_format=region_code_format,
            naming_conventions_values=naming_conventions_values,
            explorer_path=explorer_path,
            inherit_skip_properties=inherit_skip_properties,
            preview_image=preview_image,
            preview_image_singleband=preview_image_singleband,
            classifier=classifier,
            maturity=maturity,
            collection_number=collection_number,
            nodata=nodata,
        )


@dataclass
class TaskResult:
    task: Task
    result_location: str = ""
    skipped: bool = False
    error: Optional[str] = None
    meta: Any = field(init=True, repr=False, default=None)

    def __bool__(self):
        return self.error is None


@dataclass
class TaskRunnerConfig:
    @staticmethod
    def default_cog_settings():
        return dict(
            compress="deflate",
            zlevel=9,
            blocksize=800,
            ovr_blocksize=256,  # ovr_blocksize must be powers of 2 for some reason in GDAL
            overview_resampling="average",
        )

    # Input
    filedb: str
    aws_unsigned: bool = True

    # Plugin
    plugin: str = ""
    plugin_config: Dict[str, Any] = field(init=True, repr=True, default_factory=dict)

    # Output Product
    #  .{name| short_name| version| product_family|
    #    collections_site| producer| properties: Dict[str, Any]}
    product: Dict[str, Any] = field(init=True, repr=True, default_factory=dict)

    # Dask config
    threads: int = -1
    memory_limit: str = ""

    # S3/Output config
    output_location: str = ""
    s3_acl: Optional[str] = None
    # s3_public is deprecated, use s3_acl="public-read" instead
    s3_public: bool = False
    cog_opts: Dict[str, Any] = field(init=True, repr=True, default_factory=dict)
    overwrite: bool = False

    # Heartbeat filepath
    heartbeat_filepath: Optional[str] = None

    # Terminate task if running longer than this amount (seconds)
    max_processing_time: int = 0

    # tuning/testing params
    #

    # SQS renew amount (seconds)
    job_queue_max_lease: int = 5 * 60

    # Renew work token when this close to deadline (seconds)
    renew_safety_margin: int = 30

    # How often future is checked for timeout/sqs renew
    future_poll_interval: float = 5

    def __post_init__(self):
        self.cog_opts = dicttoolz.merge(self.default_cog_settings(), self.cog_opts)
