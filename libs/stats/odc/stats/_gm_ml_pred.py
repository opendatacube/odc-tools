from io import BytesIO
from typing import Dict
from typing import Tuple

import boto3
import dask.array as da
import joblib
import numpy as np
import xarray as xr
from botocore import UNSIGNED
from botocore.config import Config
from dask_ml.wrappers import ParallelPostFit
from dataclasses import dataclass
from datacube import Datacube
from datacube.testutils.io import rio_slurp_xarray
from datacube.utils.geometry import assign_crs
from odc.algo import xr_reproject
from odc.dscache.tools.tiling import GRIDS
from odc.stats.model import Task, DateTimeRange, OutputProduct, StatsPluginInterface
from pyproj import Proj, transform

from . import _plugins


def predict_xr(
        model,
        input_xr,
        chunk_size=None,
        persist=False,
        proba=False,
        clean=False,
        return_input=False,
):
    """
    Using dask-ml `ParallelPostfit()`, runs the parallel
    predict and predict_proba methods of sklearn
    estimators.

    Useful for running predictions
    on a larger-than-RAM datasets.

    Last modified: September 2020

    Parameters
    ----------
    model : scikit-learn model or compatible object
        Must have a .predict() method that takes numpy arrays.
    input_xr : xarray.DataArray or xarray.Dataset.
        Must have dimensions `x` and `y`
    chunk_size : int
        The dask chunk size to use on the flattened array. If this
        is left as None, then the chunks size is inferred from the
        .chunks method on the `input_xr`
    persist : bool
        If True, and proba=True, then `input_xr` data will be
        loaded into distributed memory. This will ensure data
        is not loaded twice for the prediction of probabilities,
        but this will only work if the data is not larger than
        distributed RAM.
    proba : bool
        If True, predict probabilities
    clean : bool
        If True, remove Infs and NaNs from input and output arrays
    return_input : bool
        If True, then the data variables in the 'input_xr' dataset will
        be appended to the output xarray dataset.

    Returns
    ----------
    output_xr : xarray.Dataset
        An xarray.Dataset containing the prediction output from model.
        if proba=True then dataset will also contain probabilites, and
        if return_input=True then dataset will have the input feature layers.
        Has the same spatiotemporal structure as input_xr.
    """
    # if input_xr isn't dask, coerce it
    dask = True
    if not bool(input_xr.chunks):
        dask = False
        input_xr = input_xr.chunk({"x": len(input_xr.x), "y": len(input_xr.y)})

    # set chunk size if not supplied
    if chunk_size is None:
        chunk_size = int(input_xr.chunks["x"][0]) * int(input_xr.chunks["y"][0])

    def _predict_func(model, input_xr, persist, proba, clean, return_input):
        x, y, crs = input_xr.x, input_xr.y, input_xr.geobox.crs

        input_data = []

        for var_name in input_xr.data_vars:
            input_data.append(input_xr[var_name])

        input_data_flattened = []

        for arr in input_data:
            data = arr.data.flatten().rechunk(chunk_size)
            input_data_flattened.append(data)

        # reshape for prediction
        input_data_flattened = da.array(input_data_flattened).transpose()

        if clean:
            input_data_flattened = da.where(
                da.isfinite(input_data_flattened), input_data_flattened, 0
            )

        if (proba == True) & (persist == True):
            # persisting data so we don't require loading all the data twice
            input_data_flattened = input_data_flattened.persist()

        # apply the classification
        print("predicting...")
        out_class = model.predict(input_data_flattened)

        # Mask out NaN or Inf values in results
        if clean:
            out_class = da.where(da.isfinite(out_class), out_class, 0)

        # Reshape when writing out
        out_class = out_class.reshape(len(y), len(x))

        # stack back into xarray
        output_xr = xr.DataArray(out_class, coords={"x": x, "y": y}, dims=["y", "x"])

        output_xr = output_xr.to_dataset(name="Predictions")

        if proba:
            print("   probabilities...")
            out_proba = model.predict_proba(input_data_flattened)

            # convert to %
            out_proba = da.max(out_proba, axis=1) * 100.0

            if clean:
                out_proba = da.where(da.isfinite(out_proba), out_proba, 0)

            out_proba = out_proba.reshape(len(y), len(x))

            out_proba = xr.DataArray(
                out_proba, coords={"x": x, "y": y}, dims=["y", "x"]
            )
            output_xr["Probabilities"] = out_proba

        if return_input:
            print("   input features...")
            # unflatten the input_data_flattened array and append
            # to the output_xr containin the predictions
            arr = input_xr.to_array()
            stacked = arr.stack(z=["y", "x"])

            # handle multivariable output
            output_px_shape = ()
            if len(input_data_flattened.shape[1:]):
                output_px_shape = input_data_flattened.shape[1:]

            output_features = input_data_flattened.reshape(
                (len(stacked.z), *output_px_shape)
            )

            # set the stacked coordinate to match the input
            output_features = xr.DataArray(
                output_features,
                coords={"z": stacked["z"]},
                dims=[
                    "z",
                    *["output_dim_" + str(idx) for idx in range(len(output_px_shape))],
                ],
            ).unstack()

            # convert to dataset and rename arrays
            output_features = output_features.to_dataset(dim="output_dim_0")
            data_vars = list(input_xr.data_vars)
            output_features = output_features.rename(
                {i: j for i, j in zip(output_features.data_vars, data_vars)}
            )

            # merge with predictions
            output_xr = xr.merge([output_xr, output_features], compat="override")

        return assign_crs(output_xr, str(crs))

    if dask:
        # convert model to dask predict
        model = ParallelPostFit(model)
        with joblib.parallel_backend("dask"):
            output_xr = _predict_func(
                model, input_xr, persist, proba, clean, return_input
            )

    else:
        output_xr = _predict_func(
            model, input_xr, persist, proba, clean, return_input
        ).compute()

    return output_xr


def read_joblib(path):
    ''' 
       Function to load a joblib file from an s3 bucket or local directory.
       Arguments:
       * path: an s3 bucket or local directory path where the file is stored
       Outputs:
       * file: Joblib file loaded
    '''

    # Path is an s3 bucket
    if path[:5] == 's3://':
        s3_bucket, s3_key = path.split('/')[2], path.split('/')[3:]
        s3_key = '/'.join(s3_key)
        with BytesIO() as f:
            boto3.client("s3", config=Config(signature_version=UNSIGNED)).download_fileobj(Bucket=s3_bucket, Key=s3_key,
                                                                                           Fileobj=f)
            f.seek(0)
            model = joblib.load(f)

    # Path is a local directory 
    else:
        with open(path, 'rb') as f:
            model = joblib.load(f)

    return model


@dataclass
class PredConf:
    PRODUCT_VERSION = '0.1.0'
    PRODUCT_NAME = "crop_mask_eastern"
    protocol = 's3://'
    s3bucket = 'deafrica-data-dev-af'
    ref_folder = 'crop_mask_references'

    model_path = f'{protocol}{s3bucket}/{ref_folder}/ml_models/gm_mads_two_seasons_ml_model_20210401.joblib'  # noqa
    url_slope = "https://deafrica-data.s3.amazonaws.com/ancillary/dem-derivatives/cog_slope_africa.tif"
    chirps_paths = (
        f'{protocol}{s3bucket}/{ref_folder}/CHIRPS/CHPclim_jan_jun_cumulative_rainfall.nc',
        f'{protocol}{s3bucket}/{ref_folder}/CHIRPS/CHPclim_jul_dec_cumulative_rainfall.nc'
    )
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
    # TODO: require dask to process, require the client
    predicted = predict_xr(
        model,
        input_data,
        chunk_size=chunk_size,
        clean=True,
        proba=True,
        return_input=True
    )

    predicted['Predictions'] = predicted['Predictions'].astype('int8')
    predicted['Probabilities'] = predicted['Probabilities'].astype('float32')

    return predicted


class PredGMS2(StatsPluginInterface):
    """
    Prediction from GeoMAD
    task run with the template
    datakube-apps/src/develop/workspaces/deafrica-dev/processing/06_stats_2019_semiannual_gm.yaml
    """
    NAME = "pred_gm_s2"
    SHORT_NAME = NAME
    VERSION = "0.1.0"
    PRODUCT_FAMILY = "geomedian"

    source_product = 'gm_s2_semiannual'
    target_product = 'crop_mask_eastern'

    def __init__(self):
        # target band to be saved
        self.bands = ('mask', 'prob')  # skip, 'filtered')
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
            time=str(PredConf.datetime_range.start.year),
            measurements=list(PredConf.rename_dict.values()),
            like=geobox,
            dask_chunks={},
        )

        dss = {"_S1": ds.isel(time=0), "_S2": ds.isel(time=1)}

        rainfall_dict = {
            '_S1': xr.open_rasterio(PredConf.chirps_paths[0]),
            '_S2': xr.open_rasterio(PredConf.chirps_paths[1])
        }

        assembled_gm_dict = dict(
            (k, gm_rainfall_single_season(dss[k], rainfall_dict[k])) for k in dss.keys()
        )

        pred_input_data = merge_two_season_feature(assembled_gm_dict, PredConf)

        model = read_joblib(PredConf.model_path)
        predicted = predict_with_model(PredConf, model, pred_input_data, {})

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
