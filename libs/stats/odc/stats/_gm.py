"""
Sentinel-2 Geomedian
"""
from typing import Optional, Tuple
import xarray as xr
import dask.array as da
import functools

import hdstats
from odc.stats.model import Task
from odc.algo.io import load_with_native_transform
from odc.algo import keep_good_only, da_yxbt_sink
from odc.algo.io import load_enum_filtered
from .model import OutputProduct, StatsPluginInterface
from . import _plugins


cloud_classes = (
    "cloud shadows",
    "cloud medium probability",
    "cloud high probability",
    "thin cirrus",
)


class StatsGMS2(StatsPluginInterface):
    def __init__(
        self,
        resampling: str = "average",
        bands: Optional[Tuple[str, ...]] = None,
        filters: Optional[Tuple[int, int]] = (2, 5),
        work_chunks: Tuple[int, int] = (100, -1),
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

        self.resampling = resampling
        self.bands = bands
        self.filters = filters
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

        measurements = tuple(self.bands)

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
        basis = task.product.measurements[0]
        chunks = {"y": -1, "x": -1}
        groupby = "solar_day"
        mask_band = "SCL"

        erased = load_enum_filtered(
            task.datasets,
            mask_band,
            task.geobox,
            categories=cloud_classes,
            filters=self.filters,
            groupby=groupby,
            resampling=self.resampling,
            chunks={},
        )

        xx = load_with_native_transform(
            task.datasets,
            task.product.measurements,
            task.geobox,
            lambda xx: xx,
            groupby=groupby,
            basis=basis,
            resampling=self.resampling,
            chunks=chunks,
        )

        xx = keep_good_only(xx, ~erased)
        return xx

    def reduce(self, xx: xr.Dataset) -> xr.Dataset:

        sample_band, *_ = xx.data_vars.values()
        nodata = sample_band.attrs.get("nodata", 0)

        bands = [dv.data for dv in xx.data_vars.values()]
        yxbt = da_yxbt_sink(bands, self._work_chunks)

        _gm_u16 = functools.partial(
            hdstats.nangeomedian_pcm,
            maxiters=1000,
            eps=1e-4,
            nocheck=True,
            nodata=nodata,
            num_threads=1,
        )

        data = da.map_blocks(
            _gm_u16, yxbt, name="geomedian", dtype="float32", drop_axis=3
        )

        coords = xx.geobox.xr_coords(with_crs=True)
        coords["band"] = list(xx.data_vars)
        gm = xr.DataArray(
            data=data, coords=coords, dims=("y", "x", "band"), attrs={"nodata": nodata}
        )

        gm = gm.to_dataset(dim="band")
        for dv in gm.data_vars.values():
            dv.attrs.update(gm.attrs)

        return gm


_plugins.register("gm-s2", StatsGMS2)
