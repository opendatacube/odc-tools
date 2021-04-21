from typing import Optional, Tuple

import xarray as xr
from dataclasses import dataclass
from odc.algo import erase_bad, geomedian_with_mads, to_rgba
from odc.algo.io import load_enum_filtered
from odc.algo.io import load_with_native_transform
from odc.stats.model import Task, DateTimeRange, OutputProduct

from . import _plugins
from ._gm import StatsGMS2


@dataclass
class PredConf:
    PRODUCT_VERSION = '0.1.0'
    PRODUCT_NAME = "crop_mask_eastern"
    protocol = 's3://'
    s3bucket = 'deafrica-data-dev-af'
    ref_folder = 'crop_mask_references'

    model_path = "https://github.com/digitalearthafrica/crop-mask/blob/main/eastern_cropmask/results/gm_mads_two_seasons_ml_model_20210401.joblib?raw=true"  # noqa
    url_slope = "https://deafrica-data.s3.amazonaws.com/ancillary/dem-derivatives/cog_slope_africa.tif"
    training_features = [
        "red_S1",
        "blue_S1",
        "green_S1",
        "nir_S1",
        "swir_1_S1",
        "swir_2_S1",
        "red_edge_1_S1",
        "red_edge_2_S1",
        "red_edge_3_S1",
        "edev_S1",
        "sdev_S1",
        "bcdev_S1",
        "NDVI_S1",
        "LAI_S1",
        "MNDWI_S1",
        "rain_S1",
        "red_S2",
        "blue_S2",
        "green_S2",
        "nir_S2",
        "swir_1_S2",
        "swir_2_S2",
        "red_edge_1_S2",
        "red_edge_2_S2",
        "red_edge_3_S2",
        "edev_S2",
        "sdev_S2",
        "bcdev_S2",
        "NDVI_S2",
        "LAI_S2",
        "MNDWI_S2",
        "rain_S2",
        "slope",
    ]
    resolution = (-10, 10)
    # the time actually is the time range, required by datacube query
    # the datetime_range is required by OutputProduct of odc-stats model
    time = ("2019-01", "2019-12")
    datetime_range = DateTimeRange(time[0], "P12M")
    output_crs = "epsg:6933"
    # query is required by open datacube
    query = {
        "time": time,
        "resolution": resolution,
        "output_crs": output_crs,
        "group_by": "solar_day",
    }
    # the prd_properties is required by the stac json
    prd_properties = {
        "odc:file_format": "GeoTIFF",
        "odc:producer": "digitalearthafrica.org",
        "odc:product": f"{PRODUCT_NAME}",
        "proj:epsg": 6933,
        "crop-mask-model": model_path,
    }
    # the OutputProduct is required by stac json
    product = OutputProduct(
        name=PRODUCT_NAME,
        version=PRODUCT_VERSION,
        short_name=PRODUCT_NAME,
        location=f'{protocol}{s3bucket}/',
        properties=prd_properties,
        measurements=("mask", "prob", "filtered"),
        href=f"https://explorer.digitalearth.africa/products/{PRODUCT_NAME}",
    )


class PredGMS2(StatsGMS2):
    """
    Prediction from GeoMAD
    task run with the template
    datakube-apps/src/develop/workspaces/deafrica-dev/processing/06_stats_2019_semiannual_gm.yaml
    """
    NAME = "pred_gm_s2"
    SHORT_NAME = NAME
    VERSION = "0.1.0"
    PRODUCT_FAMILY = "geomedian"
    chirps_paths = (
        f'{PredConf.protocol}{PredConf.s3bucket}/{PredConf.ref_folder}/CHIRPS/CHPclim_jan_jun_cumulative_rainfall.nc',
        f'{PredConf.protocol}{PredConf.s3bucket}/{PredConf.ref_folder}/CHIRPS/CHPclim_jul_dec_cumulative_rainfall.nc'
    )

    def __init__(
            self,
            bands: Optional[Tuple[str, ...]] = None,
            mask_band: Optional[str] = None,
            cloud_classes: Tuple[str, ...] = None,
            nodata_classes: Optional[Tuple[str, ...]] = None,
            filters: Optional[Tuple[int, int]] = None,
            basis_band: Optional[str] = None,
            aux_names=None,
            rgb_bands=None,
            rgb_clamp=None,
            resampling: str = "bilinear",
            work_chunks: Tuple[int, int] = (400, 400),
            **other,
    ):
        if bands is None:
            # target band to be saved
            bands = ('mask', 'prob', 'filtered')

        super().__init__(
            bands=bands,
            mask_band=mask_band,
            cloud_classes=cloud_classes,
            nodata_classes=nodata_classes,
            filters=filters,
            basis_band=basis_band,
            aux_names=aux_names,
            rgb_bands=rgb_bands,
            rgb_clamp=rgb_clamp,
            resampling=resampling,
            work_chunks=work_chunks,
            **other,
        )

    @property
    def measurements(self) -> Tuple[str, ...]:
        return self.bands

    def _native_op_data_band(self, xx):
        from odc.algo import enum_to_bool, keep_good_only

        if not self._mask_band in xx.data_vars:
            return xx

        # Erase Data Pixels for which mask == nodata
        #
        #  xx[mask == nodata] = nodata
        mask = xx[self._mask_band]
        xx = xx.drop_vars([self._mask_band])
        keeps = enum_to_bool(mask, self._nodata_classes, invert=True)
        xx = keep_good_only(xx, keeps)

        return xx

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

        bands_to_load = self.bands
        if self._nodata_classes is not None:
            # NOTE: this ends up loading Mask band twice, once to compute
            # ``.erase`` band and once to compute ``nodata`` mask.
            bands_to_load = (*bands_to_load, self._mask_band)

        xx = load_with_native_transform(
            task.datasets,
            bands_to_load,
            task.geobox,
            self._native_op_data_band,
            groupby=groupby,
            basis=basis,
            resampling=self.resampling,
            chunks=chunks,
        )

        xx = erase_bad(xx, erased)
        return xx

    def reduce(self, xx: xr.Dataset) -> xr.Dataset:
        scale = 1 / 10_000
        cfg = dict(
            maxiters=1000,
            num_threads=1,
            scale=scale,
            offset=-1 * scale,
            reshape_strategy="mem",
            out_chunks=(-1, -1, -1),
            work_chunks=self._work_chunks,
            compute_count=True,
            compute_mads=True,
        )

        gm = geomedian_with_mads(xx, **cfg)
        gm = gm.rename(self._renames)

        return gm

    def rgba(self, xx: xr.Dataset) -> Optional[xr.DataArray]:
        if self.rgb_bands is None:
            return None
        return to_rgba(xx, clamp=self.rgb_clamp, bands=self.rgb_bands)


_plugins.register("pred-gm-s2", PredGMS2)
