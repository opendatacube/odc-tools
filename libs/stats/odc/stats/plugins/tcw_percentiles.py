"""
Fractional Cover Percentiles
"""
from functools import partial
from typing import Optional, Sequence, Tuple, Dict
import xarray as xr
import numpy as np
from odc.algo import keep_good_only
from odc.algo._percentile import xr_quantile_bands
from odc.algo._masking import _xr_fuse, _fuse_mean_np, enum_to_bool
from ._registry import StatsPluginInterface, register

NODATA = -9999 # output NODATA


class StatsTCWPC(StatsPluginInterface):

    NAME = "ga_tcw_percentiles"
    SHORT_NAME = NAME
    VERSION = "0.0.1"
    PRODUCT_FAMILY = "twc_percentiles"

    def __init__(
        self,
        coefficients: Dict[str, float] = {
            'blue': 0.0315, 'green': 0.2021, 'red': 0.3102, 'nir': 0.1594, 'swir1': -0.6806, 'swir2': -0.6109
            },
        input_bands: Sequence[str] = ["blue", "green", "red", "nir", "swir1", "swir2", "fmask", "nbart_contiguity"],
        **kwargs
    ):
        super().__init__(input_bands=input_bands, **kwargs)
        self.coefficients = coefficients

    @property
    def measurements(self) -> Tuple[str, ...]:
        _measurments = ["wet_pc_10", "wet_pc_50", "wet_pc_90"]
        return _measurments

    def native_transform(self, xx):
        """
        Loads data in its native projection.
        """
        bad = enum_to_bool(xx["fmask"], ("nodata", "cloud", "shadow")) # a pixel is bad if any of the cloud, shadow, or no-data value
        bad |= xx["nbart_contiguity"] == 0 # or the nbart contiguity bit is 0
        xx = xx.drop_vars(["fmask", "nbart_contiguity"])
        
        for band in xx.data_vars.keys():
            bad = bad | (xx[band] == -999)

        tcw = sum(coeff * xx[band] for band, coeff in self.coefficients.items())
        tcw.attrs = xx.blue.attrs
        tcw.attrs['nodata'] = NODATA
        
        xx = xx.drop_vars(xx.data_vars.keys())
        xx['wet'] = tcw.astype(np.int16)
        xx = keep_good_only(xx, ~bad, nodata=NODATA)
        return xx

    @staticmethod
    def fuser(xx):
        xx = _xr_fuse(xx, partial(_fuse_mean_np, nodata=NODATA), '')

        return xx
    
    @staticmethod
    def reduce(xx: xr.Dataset) -> xr.Dataset:
        yy = xr_quantile_bands(xx, [0.1, 0.5, 0.9], nodata=NODATA)
        return yy


register("tcw-percentiles", StatsTCWPC)
