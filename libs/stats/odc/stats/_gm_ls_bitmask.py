"""
Landsat QA Pixel Geomedian
"""
from functools import partial
from typing import Dict, Optional, Tuple, Any, Iterable

import xarray as xr
import numpy as np
from datacube.utils import masking
from odc.algo import geomedian_with_mads, keep_good_only, erase_bad
from odc.algo._masking import _xr_fuse, _first_valid_np, mask_cleanup, _fuse_or_np
from odc.algo.io import load_with_native_transform
from odc.stats import _plugins
from odc.stats.model import StatsPluginInterface
from odc.stats.model import Task

class StatsGMLSBitmask(StatsPluginInterface):
    NAME = "gm_ls_bitmask"
    SHORT_NAME = NAME
    VERSION = "0.0.1"

    def __init__(
            self,
            bands: Optional[Tuple[str, ...]] = None,
            mask_band: str = "QA_PIXEL",
            # provide flags with high cloud bits definition
            flags: Dict[str, Optional[Any]] = dict(
                cloud="high_confidence",
                cirrus="high_confidence",
            ),
            nodata_flags: Dict[str, Optional[Any]] = dict(nodata=False),
            filters: Optional[Iterable[Tuple[str, int]]] = None, # e.g. [("closing", 10),("opening", 2),("dilation", 2)]
            aux_names=dict(smad="smad", emad="emad", bcmad="bcmad", count="count"),
            resampling: str = "bilinear",
            work_chunks: Tuple[int, int] = (400, 400),
            scale: float = 0.0000275,
            offset: float = -0.2,
            output_scale: int = 10000, # gm rescaling - making SR range match sentinel-2 gm
            output_dtype: str = "uint16", # dtype of gm rescaling
            **other,
    ):
        self.mask_band = mask_band
        self.resampling = resampling
        self.bands = bands
        self.flags = flags
        self.nodata_flags = nodata_flags
        self.filters = filters
        self.work_chunks = work_chunks
        self.renames = aux_names
        self.aux_bands = list(aux_names.values())
        self.scale = scale
        self.offset = offset
        self.output_scale = output_scale
        self.output_dtype = np.dtype(output_dtype)
        self.output_nodata = 0

        if self.bands is None:
            self.bands = (
                "red",
                "green",
                "blue",
                "nir",
                "swir_1",
                "swir_2",
            )

    @property
    def measurements(self) -> Tuple[str, ...]:
        return self.bands + self.aux_bands

    def _native_tr(self, xx):
        """
        Loads in the data in the native projection. It performs the following:

        1. Loads pq bands
        2. Extract valid - by removing negative pixel using masking_scale from input bands
        3. Extract cloud_mask flags from bands
        4. Drop nodata and negative pixels
        5. Add cloud_mask band to xx for fuser and reduce

        .. bitmask::
            15  14  13  12  11  10  9   8   7   6   5   4   3   2   1   0
            |   |   |   |   |   |   |   |   |   |   |   |   |   |   |   |
            |   |   |   |   |   |   |   |   |   |   |   |   |   |   |   x-----> nodata
            |   |   |   |   |   |   |   |   |   |   |   |   |   |   o---------> dilated_cloud
            |   |   |   |   |   |   |   |   |   |   |   |   |   x-------------> cirrus
            |   |   |   |   |   |   |   |   |   |   |   |   o-----------------> cloud
            |   |   |   |   |   |   |   |   |   |   |   x---------------------> cloud_shadow
            |   |   |   |   |   |   |   |   |   |   o-------------------------> snow
            |   |   |   |   |   |   |   |   |   x-----------------------------> clear
            |   |   |   |   |   |   |   |   o---------------------------------> water
            |   |   |   |   |   |   |   x-------------------------------------> cloud_confidence
            |   |   |   |   |   |   o-----------------------------------------> cloud_confidence
            |   |   |   |   |   x---------------------------------------------> cloud_shadow_confidence
            |   |   |   |   o-------------------------------------------------> cloud_shadow_confidence
            |   |   |   x-----------------------------------------------------> snow_ice_confidence
            |   |   o---------------------------------------------------------> snow_ice_confidence
            |   x-------------------------------------------------------------> cirrus_confidence
            0-----------------------------------------------------------------> cirrus_confidence
        """

        # remove negative pixels - a pixel is invalid if any of the band is smaller than masking_scale
        valid = (xx[self.bands] > (-1.0 * self.offset/self.scale)).to_array(dim='band').all(dim='band')

        mask_band = xx[self.mask_band]
        xx = xx.drop_vars([self.mask_band])

        flags_def = masking.get_flags_def(mask_band)

        # set cloud_mask - True=cloud, False=non-cloud
        mask, _ = masking.create_mask_value(flags_def, **self.flags)
        cloud_mask = (mask_band & mask) != 0

        # set no_data bitmask - True=data, False=no-data
        nodata_mask, _ = masking.create_mask_value(flags_def, **self.nodata_flags)
        keeps = (mask_band & nodata_mask) == 0

        xx = keep_good_only(xx, valid)   # remove negative pixels
        xx = keep_good_only(xx, keeps)   # remove nodata pixels
        xx["cloud_mask"] = cloud_mask

        return xx

    def input_data(self, task: Task) -> xr.Dataset:

        chunks = {"y": -1, "x": -1}

        xx = load_with_native_transform(
            task.datasets,
            bands=self.bands + [self.mask_band],
            geobox=task.geobox,
            native_transform=self._native_tr,
            fuser=self._fuser,
            groupby="solar_day",
            resampling=self.resampling,
            chunks=chunks,
        )

        return xx

    def reduce(self, xx: xr.Dataset) -> xr.Dataset:
        cloud_mask = xx["cloud_mask"]
        cfg = dict(
            maxiters=1000,
            num_threads=1,
            scale=self.scale,
            offset=self.offset,
            reshape_strategy="mem",
            out_chunks=(-1, -1, -1),
            work_chunks=self.work_chunks,
            compute_count=True,
            compute_mads=True,
        )

        if self.filters is not None:
            cloud_mask = mask_cleanup(xx["cloud_mask"], mask_filters=self.filters)

        # erase pixels with cloud
        xx = xx.drop_vars(["cloud_mask"])
        xx = erase_bad(xx, cloud_mask)

        gm = geomedian_with_mads(xx, **cfg)
        gm = gm.rename(self.renames)

        # rescale gm bands into surface reflectance scale
        for band in gm.data_vars.keys():
            if band in self.bands or band == self.renames['emad']:
                # set nodata_mask - use for resetting nodata pixel after rescale
                nodata_mask = gm[band] == gm[band].attrs.get('nodata')
                # rescale
                if band == self.renames['emad']:
                    gm[band] = self.scale * self.output_scale * gm[band]
                else:
                    gm[band] = self.scale * self.output_scale * gm[band] + self.offset * self.output_scale
                #  apply nodata_mask - reset nodata pixels to output-nodata
                gm[band] = gm[band].where(~nodata_mask, self.output_nodata)
                # set data-type and nodata attrs
                gm[band] = gm[band].round().astype(self.output_dtype)
                gm[band].attrs['nodata'] = self.output_nodata

        return gm

    def _fuser(self, xx):
        """
        Fuse cloud_mask with OR
        """
        cloud_mask = xx["cloud_mask"]
        xx = _xr_fuse(xx.drop_vars(["cloud_mask"]), partial(_first_valid_np, nodata=0), '')
        xx["cloud_mask"] = _xr_fuse(cloud_mask, _fuse_or_np, cloud_mask.name)

        return xx

    def rgba(self, xx: xr.Dataset) -> Optional[xr.DataArray]:
        return None


_plugins.register("gm-ls-bitmask", StatsGMLSBitmask)
