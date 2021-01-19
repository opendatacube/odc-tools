"""
Sentinel-2 Geomedian
"""
from typing import Optional, Tuple
import xarray as xr
from odc.stats.model import Task
from odc.algo.io import load_with_native_transform
from odc.algo import erase_bad, yxbt_sink, to_rgba
from odc.algo.io import load_enum_filtered
from .model import OutputProduct, StatsPluginInterface
from . import _plugins


class StatsGMS2(StatsPluginInterface):
    def __init__(
        self,
        resampling: str = "bilinear",
        bands: Optional[Tuple[str, ...]] = None,
        filters: Optional[Tuple[int, int]] = (2, 5),
        work_chunks: Tuple[int, int] = (400, 400),
        mask_band: str = "SCL",
        cloud_classes=(
            "cloud shadows",
            "cloud medium probability",
            "cloud high probability",
            "thin cirrus",
        ),
        basis_band=None,
        aux_names=dict(smad="SMAD", emad="EMAD", bcmad="BCMAD", count="COUNT"),
        rgb_bands=None,
        rgb_clamp=(1, 3_000),
    ):
        if bands is None:
            bands = (
                "B02",
                "B03",
                "B04",
                "B05",
                "B06",
                "B07",
                "B08",
                "B8A",
                "B11",
                "B12",
            )
            if rgb_bands is None:
                rgb_bands = ("B04", "B03", "B02")

        self.resampling = resampling
        self.bands = tuple(bands)
        self._basis_band = basis_band or self.bands[0]
        self._renames = aux_names
        self.rgb_bands = rgb_bands
        self.rgb_clamp = rgb_clamp
        self.aux_bands = tuple(
            self._renames.get(k, k) for k in ("smad", "emad", "bcmad", "count")
        )
        self._mask_band = mask_band
        self.filters = filters
        self.cloud_classes = tuple(cloud_classes)

        self._work_chunks = (*work_chunks, -1, -1)

    def product(self, location: Optional[str] = None, **kw) -> OutputProduct:
        name = "ga_s2_gm"
        short_name = "ga_s2_gm"
        version = "0.0.0"

        if location is None:
            bucket = "deafrica-stats-processing"
            location = f"s3://{bucket}/{name}/v{version}"
        else:
            location = location.rstrip("/")

        measurements = self.bands + self.aux_bands

        properties = {
            "odc:file_format": "GeoTIFF",
            "odc:producer": "ga.gov.au",
            "odc:product_family": "statistics",  # TODO: ???
            "platform": "sentinel-2",
        }

        return OutputProduct(
            name=name,
            version=version,
            short_name=short_name,
            location=location,
            properties=properties,
            measurements=measurements,
            href=f"https://collections.digitalearth.africa/product/{name}",
        )

    def input_data(self, task: Task) -> xr.Dataset:
        basis = self._basis_band
        chunks = {"y": -1, "x": -1}
        groupby = "solar_day"

        erased = load_enum_filtered(
            task.datasets,
            self._mask_band,
            task.geobox,
            categories=self.cloud_classes,
            filters=self.filters,
            groupby=groupby,
            resampling=self.resampling,
            chunks={},
        )

        xx = load_with_native_transform(
            task.datasets,
            self.bands,
            task.geobox,
            lambda xx: xx,
            groupby=groupby,
            basis=basis,
            resampling=self.resampling,
            chunks=chunks,
        )

        xx = erase_bad(xx, erased)
        return xx

    def reduce(self, xx: xr.Dataset) -> xr.Dataset:
        from odc.algo._geomedian import geomedian_with_mads

        yxbt = yxbt_sink(xx, self._work_chunks)

        scale = 1 / 10_000
        cfg = dict(
            maxiters=1000,
            num_threads=1,
            scale=scale,
            offset=-1 * scale,
            out_chunks=(-1, -1, -1),
        )

        gm = geomedian_with_mads(yxbt, **cfg)
        gm = gm.rename(self._renames)

        return gm

    def rgba(self, xx: xr.Dataset) -> Optional[xr.DataArray]:
        if self.rgb_bands is None:
            return None
        return to_rgba(xx, clamp=self.rgb_clamp, bands=self.rgb_bands)


_plugins.register("gm-s2", StatsGMS2)
