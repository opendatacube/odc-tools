from abc import ABC, abstractmethod, abstractproperty
import math
from pathlib import Path
from copy import deepcopy
import tempfile
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Optional, Tuple, Union
from uuid import UUID
from toolz import dicttoolz
import pystac
import xarray as xr
import pandas as pd
from datacube.model import Dataset
from datacube.utils.dates import normalise_dt
from datacube.utils.geometry import GeoBox
from datacube.testutils.io import native_geobox
from odc.index import odc_uuid
from odc.io.text import split_and_check
from eodatasets3.assemble import DatasetAssembler
from eodatasets3.model import DatasetDoc, ProductDoc
from eodatasets3.properties import StacPropertyView

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
        return tuple(ds.id for ds in self.datasets)

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

    def _native_resolution(self) -> float:
        geobox = native_geobox(
            self.datasets[0], basis=list(self.datasets[0].measurements.keys())[0]
        )
        return geobox.affine[0]

    def _convert_eo_plus(self, ds) -> DatasetDoc:
        # Definitely need: # - 'datetime' # - 'eo:instrument' # - 'eo:platform' # - 'odc:region_code'
        region_code = _guess_region_code(ds)
        properties = StacPropertyView(
            {
                "odc:region_code": region_code,
                "datetime": ds.center_time,
                "eo:instrument": ds.metadata.instrument,
                "eo:platform": ds.metadata.platform,
                "landsat:landsat_scene_id": ds.metadata_doc.get(
                    "tile_id", "??"
                ),  # Used to find abbreviated instrument id
                "sentinel:sentinel_tile_id": ds.metadata_doc.get("tile_id", "??"),
            }
        )
        product = ProductDoc(name=ds.type.name)
        return DatasetDoc(id=ds.id, product=product, crs=str(ds.crs), properties=properties)

    def _convert_eo(self, ds) -> DatasetDoc:
        # Definitely need: # - 'datetime' # - 'eo:instrument' # - 'eo:platform' # - 'odc:region_code'
        region_code = _guess_region_code(ds)
        properties = StacPropertyView(
            {
                "odc:region_code": region_code,
                "datetime": ds.center_time,
                "eo:instrument": ds.metadata.instrument,
                "eo:platform": ds.metadata.platform,
                "landsat:landsat_scene_id": ds.metadata.instrument,  # Used to find abbreviated instrument id
            }
        )
        product = ProductDoc(name=ds.type.name)
        return DatasetDoc(id=ds.id, product=product, crs=str(ds.crs), properties=properties)

    def _munge_dataset_to_eo3(self) -> DatasetDoc:
        """
        Convert to the DatasetDoc format that eodatasets expects.
        """

        ds = self.datasets[0]

        if ds.metadata_type.name in {"eo_plus", "eo_s2_nrt", "gqa_eo"}:
            # Handle S2 NRT metadata identically to eo_plus files.
            # gqa_eo is the S2 ARD with extra quality check fields.
            return _convert_eo_plus(ds)

        if ds.metadata_type.name == "eo":
            return _convert_eo(ds)

        # Else we have an already mostly eo3 style dataset
        product = ProductDoc(name=ds.type.name)
        # Wrap properties to avoid typos and the like
        properties = StacPropertyView(ds.metadata_doc.get("properties", {}))
        if properties.get("eo:gsd"):
            del properties["eo:gsd"]
        return DatasetDoc(
            id=ds.id,
            product=product,
            crs=str(ds.crs),
            properties=properties,
            geometry=ds.extent,
        )

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


    def render_odc_metadata(
        self, ext: str = EXT_TIFF, processing_dt: Optional[datetime] = None
    ) -> Dict[str, Any]:
        """
        Put together ODC metadata document for the output of this task. 
        """
        # To the ODC metadata, it needs info
        # 1) $schema: https://schemas.opendatacube.org/dataset (maybe hard-code in EO Dataset3) [?] -> hard-code source?
        # 2) id: it is the uuid of this dataset [/]
        # 3) label: output file name pattern [/]
        # 4) product and href: from plug? [?] -> external source?
        # 5) crs: [/]
        # 6) gemoetry [/]
        # 7) grids -> defualt -> shape -> transform [/]
        # 8) properties [/]
        # 9) accessories -> thumbnail -> checksum:sha1 -> metadata:processor [?] -> EO Dataset method
        # 10) lineage [/]
        
        temp_metadata_path = Path(tempfile.gettempdir()) / f"{self.uuid}.yaml"

        source_dataset = self._munge_dataset_to_eo3()

        #for prepertie in source_dataset.properties:
        #    print('prepertie', prepertie)

        for k,v in source_dataset.grids:
            print('Grid Spec', k, v)

        with DatasetAssembler(
            metadata_path=temp_metadata_path,
            naming_conventions="dea_c3", # move to Config file
            dataset_id=self.uuid,
        ) as dataset_assembler:
            dataset_assembler.add_source_dataset(
                self._munge_dataset_to_eo3(), # because it is the summary, so we only grab the first input dataset to inherit the properties.
                auto_inherit_properties=True,
                inherit_geometry=True, # always inerit from input dataset
                classifier="wo", # move to Config file
            )

            # Copy in metadata and properties
            #for k, v in task.settings.output.metadata.items():
            #    setattr(dataset_assembler, k, v)

            #if task.settings.output.properties:
            #    for k, v in task.settings.output.properties.items():
            #        dataset_assembler.properties[k] = v

            # I believe they should be inherited from input dataset?
            outside_properties = {
                'dea:dataset_maturity': 'final',
                'odc:collection_number': '3',
                'odc:dataset_version': '1.6.0',
                'odc:producer': 'ga.gov.au',
                'odc:product_family': 'wo' 
            }

            for k, v in outside_properties.items():
                dataset_assembler.properties[k] = v

            # Update the GSD
            dataset_assembler.properties["eo:gsd"] = self._native_resolution()

            if processing_dt is None:
                processing_dt = datetime.utcnow()

            dataset_assembler.processed = processing_dt
    
            # Write it all to a tempdir root, and then either shift or s3 sync it into place
            with tempfile.TemporaryDirectory() as temp_dir:
                # Set up a temporary directory
                dataset_assembler.collection_location = Path(temp_dir)

                 # Dodgy hack!
                dataset_assembler._metadata_path = None

                # Do all the deferred work from above
                dataset_id, metadata_path = dataset_assembler.done()
                log.info("Assembled dataset", metadata_path=metadata_path)

                relative_path = dataset_assembler._dataset_location.relative_to(
                    temp_dir
                )

                dest_directory = "/home/ubuntu/odc-stats-test-data"

                log.info("Writing files to disk", location=dest_directory)
                if dest_directory.exists():
                    shutil.rmtree(dest_directory)
                shutil.copytree(
                    dataset_assembler._dataset_location, dest_directory
                )
                log.info("Task complete")

    def render_stac_metadata(
        self, ext: str = EXT_TIFF, processing_dt: Optional[datetime] = None
    ) -> Dict[str, Any]:
        """
        Put together STAC metadata document for the output of this task. It will be replaced by
        dc_to_stac() in EO Datasets3.
        """
        if processing_dt is None:
            processing_dt = datetime.utcnow()

        product = self.product
        geobox = self.geobox
        region_code = product.region_code(self.tile_index)
        inputs = list(map(str, self._lineage()))

        properties: Dict[str, Any] = deepcopy(product.properties)

        properties["dtr:start_datetime"] = format_datetime(self.time_range.start)
        properties["dtr:end_datetime"] = format_datetime(self.time_range.end)
        properties["odc:processing_datetime"] = format_datetime(
            processing_dt, timespec="seconds"
        )
        properties["odc:region_code"] = region_code
        properties["odc:product"] = product.name
        properties["odc:dataset_version"] = product.version

        geobox_wgs84 = geobox.extent.to_crs(
            "epsg:4326", resolution=math.inf, wrapdateline=True
        )
        bbox = geobox_wgs84.boundingbox

        item = pystac.Item(
            id=str(self.uuid),
            geometry=geobox_wgs84.json,
            bbox=[bbox.left, bbox.bottom, bbox.right, bbox.top],
            datetime=self.time_range.start.replace(tzinfo=timezone.utc),
            properties=properties,
            stac_extensions=["projection"],
        )

        item.ext.projection.epsg = geobox.crs.epsg
        # Lineage last
        item.properties["odc:lineage"] = dict(inputs=inputs)

        # Add all the assets
        for band, path in self.paths(ext=ext).items():
            asset = pystac.Asset(
                href=path,
                media_type="image/tiff; application=geotiff",
                roles=["data"],
                title=band,
            )
            item.add_asset(band, asset)

            item.ext.projection.set_transform(geobox.transform, asset=asset)
            item.ext.projection.set_shape(geobox.shape, asset=asset)

        # Add links
        item.links.append(
            pystac.Link(
                rel="product_overview",
                media_type="application/json",
                target=product.href,
            )
        )
        item.links.append(
            pystac.Link(
                rel="self",
                media_type="application/json",
                target=self.metadata_path("absolute", ext="json"),
            )
        )

        return item.to_dict()


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
