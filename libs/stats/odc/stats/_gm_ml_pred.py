from typing import Dict, Optional
from typing import Tuple

import fsspec
import joblib
import numpy as np
import xarray as xr
from dataclasses import dataclass
from datacube import Datacube
from datacube.testutils.io import rio_slurp_xarray
from datacube.utils.geometry import assign_crs
from deafrica_tools.classification import predict_xr
from odc.algo import xr_reproject
from odc.dscache.tools.tiling import GRIDS
from odc.stats.model import Task, DateTimeRange, OutputProduct
from pyproj import Proj, transform

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
    rename_dict = {  # "nir_1": "nir",
        "B02": "blue",
        "B03": "green",
        "B04": "red",
        "B05": "red_edge_1",
        "B06": "red_edge_2",
        "B07": "red_edge_3",
        "B08": "nir",
        "B8A": "nir_narrow",
        "B11": "swir_1",
        "B12": "swir_2",
        "BCMAD": "bcdev",
        "EMAD": "edev",
        "SMAD": "sdev",
    }
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


def down_scale_gm_band(
        ds: xr.Dataset, exclude: Tuple[str, str] = ("sdev", "bcdev"), scale=10_000
) -> xr.Dataset:
    """
    down scale band not in exclude list.
    :param ds:
    :param exclude:
    :param scale:
    :return:
    """
    for band in ds.data_vars:
        if band not in exclude:
            ds[band] = ds[band] / scale
    return ds


def chirp_clip(ds: xr.Dataset, chirps: xr.DataArray) -> xr.DataArray:
    """
     fill na with mean on chirps data
    :param ds: geomedian collected with certain geobox
    :param chirps: rainfall data
    :return: chirps data without na
    """
    # TODO: test with dummy ds and chirps
    # Clip CHIRPS to ~ S2 tile boundaries so we can handle NaNs local to S2 tile
    xmin, xmax = ds.x.values[0], ds.x.values[-1]
    ymin, ymax = ds.y.values[0], ds.y.values[-1]
    inProj = Proj("epsg:6933")
    outProj = Proj("epsg:4326")
    xmin, ymin = transform(inProj, outProj, xmin, ymin)
    xmax, ymax = transform(inProj, outProj, xmax, ymax)

    # create lat/lon indexing slices - buffer S2 bbox by 0.05deg
    # Todo: xmin < 0 and xmax < 0,  x_slice = [], unit tests
    if (xmin < 0) & (xmax < 0):
        x_slice = list(np.arange(xmin + 0.05, xmax - 0.05, -0.05))
    else:
        x_slice = list(np.arange(xmax - 0.05, xmin + 0.05, 0.05))

    if (ymin < 0) & (ymax < 0):
        y_slice = list(np.arange(ymin + 0.05, ymax - 0.05, -0.05))
    else:
        y_slice = list(np.arange(ymin - 0.05, ymax + 0.05, 0.05))

    # index global chirps using buffered s2 tile bbox
    chirps = assign_crs(chirps.sel(x=y_slice, y=x_slice, method="nearest"))

    # fill any NaNs in CHIRPS with local (s2-tile bbox) mean
    return chirps.fillna(chirps.mean())


def calculate_indices(ds: xr.Dataset) -> xr.Dataset:
    """
    add calculate_indices into the datasets
    :param ds: input ds with nir, red, green bands
    :return: ds with new bands
    """
    inices_dict = {
        "NDVI": lambda ds: (ds.nir - ds.red) / (ds.nir + ds.red),
        "LAI": lambda ds: (
                3.618
                * ((2.5 * (ds.nir - ds.red)) / (ds.nir + 6 * ds.red - 7.5 * ds.blue + 1))
                - 0.118
        ),
        "MNDWI": lambda ds: (ds.green - ds.swir_1) / (ds.green + ds.swir_1),
    }

    for k, func in inices_dict.items():
        ds[k] = func(ds)

    ds["sdev"] = -np.log(ds["sdev"])
    ds["bcdev"] = -np.log(ds["bcdev"])
    ds["edev"] = -np.log(ds["edev"])

    return ds


def gm_rainfall_single_season(
        geomedian_with_mads: xr.Dataset,
        rainfall: xr.DataArray,
) -> xr.Dataset:
    """
    generate gm-semiannual with rainfall, query sample see bellow
    :param dc: Datacube context
    :param query: require fields above
    :param season_time_dict: define the time range for each crop season
    :param rainfall_dict: cache the rainfall data with dict
    :param season_key: one of {'_S1', '_S2'}
    :return: gm with rainfall
    """

    # remove time dim
    geomedian_with_mads = geomedian_with_mads.drop("time")
    # scale
    geomedian_with_mads = down_scale_gm_band(geomedian_with_mads)
    geomedian_with_mads = assign_crs(calculate_indices(geomedian_with_mads))

    # rainfall
    rainfall = assign_crs(rainfall, crs="epsg:4326")
    rainfall = chirp_clip(geomedian_with_mads, rainfall)

    rainfall = (
        xr_reproject(rainfall, geomedian_with_mads.geobox, "bilinear")
            .drop(["band", "spatial_ref"])
            .squeeze()
    )

    geomedian_with_mads["rain"] = rainfall

    return geomedian_with_mads.drop("spatial_ref").squeeze()


def merge_two_season_feature(
        seasoned_ds: Dict[str, xr.Dataset], config: dataclass
) -> xr.Dataset:
    """
    combine the two season datasets and add slope to build the machine learning feature
    :param seasoned_ds:  gm+indices+rainfall
    :param config: FeaturePathConfig has slop url
    :return: merged xr Dataset
    """
    slope = (
        rio_slurp_xarray(config.url_slope, gbox=seasoned_ds["_S1"].geobox)
            .drop("spatial_ref")
            .to_dataset(name="slope")
    )
    renamed_seasoned_ds = {}
    for k, v in seasoned_ds.items():
        renamed_seasoned_ds[k] = v.rename(dict((str(band), str(band) + k) for band in v.data_vars))

    return xr.merge(
        [renamed_seasoned_ds["_S1"], renamed_seasoned_ds["_S2"], slope], compat="override"
    ).chunk({"x": -1, "y": -1})


def predict_with_model(config, model, data: xr.Dataset, chunk_size=None) -> xr.Dataset:
    """
    run the prediction here
    """
    # step 1: select features
    input_data = data[config.training_features]

    # step 2: prediction
    predicted = predict_xr(
        model,
        input_data,
        chunk_size=chunk_size,
        clean=True,
        proba=True,
        return_input=True
    ).compute()

    predicted['Predictions'] = predicted['Predictions'].astype('int8')
    predicted['Probabilities'] = predicted['Probabilities'].astype('float32')

    return predicted


class PredGMS2(StatsGMS2):
    """
    Prediction from GeoMAD
    task run with the template
    datakube-apps/src/develop/workspaces/deafrica-dev/processing/06_stats_2019_semiannual_gm.yaml
    TODO: target product derived from source product.
    TODO: use half year task messages from semiannual to do the prediction
    """
    NAME = "pred_gm_s2"
    SHORT_NAME = NAME
    VERSION = "0.1.0"
    PRODUCT_FAMILY = "geomedian"
    chirps_paths = (
        f'{PredConf.protocol}{PredConf.s3bucket}/{PredConf.ref_folder}/CHIRPS/CHPclim_jan_jun_cumulative_rainfall.nc',
        f'{PredConf.protocol}{PredConf.s3bucket}/{PredConf.ref_folder}/CHIRPS/CHPclim_jul_dec_cumulative_rainfall.nc'
    )
    source_product = 'gm_s2_semiannual'
    target_product = 'crop_mask_eastern'

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
            bands = ('mask', 'prob')  # skip, 'filtered')

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
        self.africa_N = GRIDS[f'africa_{PredConf.resolution[1]}']

    @property
    def measurements(self) -> Tuple[str, ...]:
        return self.bands

    def input_data(self, task: Task) -> xr.Dataset:
        """
        assemble the input data and do prediction here.
        This method work as pipeline
        """
        dc = Datacube(app=self.target_product)
        geobox = self.africa_N.tile_geobox(task.tile_index)
        ds = dc.load(
            product=self.source_product,
            time=PredConf.datetime_range.year,
            measurements=list(PredConf.rename_dict.values()),
            like=geobox,
            dask_chunks={},
            resampling=self.resampling,
        )
        dss = {"_S1": ds.isel(time=0), "_S2": ds.isel(time=1)}

        rainfall_dict = {
            '_S1': xr.open_rasterio(self.chirps_paths[0]),
            '_S2': xr.open_rasterio(self.chirps_paths[1])
        }

        assembled_gm_dict = dict(
            (k, gm_rainfall_single_season(dss[k], rainfall_dict[k])) for k in dss.keys()
        )

        pred_input_data = merge_two_season_feature(assembled_gm_dict, PredConf)

        with fsspec.open(PredConf.model_path) as fh:
            model = joblib.load(fh)

        predicted = predict_with_model(PredConf, model, pred_input_data, {})
        # predicted, prob, filtered = post_processing(pred_input_data, predicted, geobox)
        output_ds = xr.Dataset(
            {
                'mask': predicted['Predictions'],
                'prob': predicted['Probabilities']
            }
        )
        return output_ds

    def reduce(self, xx: xr.Dataset) -> xr.Dataset:
        return xx


_plugins.register("pred-gm-s2", PredGMS2)
