"""
Geomedian
"""
from typing import Optional, Sequence, Tuple, Iterable
import xarray as xr
from datacube.model import Dataset
from datacube.utils.geometry import GeoBox
from odc.algo import erase_bad, geomedian_with_mads
from odc.algo.io import load_enum_filtered
from ._registry import StatsPluginInterface, register


class StatsGM(StatsPluginInterface):
    NAME = "gm"
    SHORT_NAME = NAME
    VERSION = "0.0.0"
    PRODUCT_FAMILY = "geomedian"

    def __init__(
        self,
        bands: Tuple[str, ...],
        mask_band: str,
        cloud_classes: Tuple[str, ...],
        nodata_classes: Optional[Tuple[str, ...]] = None,
        filters: Optional[Iterable[Tuple[str, int]]] = None,
        basis_band=None,
        aux_names=dict(smad="smad", emad="emad", bcmad="bcmad", count="count"),
        work_chunks: Tuple[int, int] = (400, 400),
        **kwargs,
    ):
        self.bands = tuple(bands)
        self._mask_band = mask_band
        self._nodata_classes = nodata_classes
        input_bands = self.bands
        if self._nodata_classes is not None:
            # NOTE: this ends up loading Mask band twice, once to compute
            # ``.erase`` band and once to compute ``nodata`` mask.
            input_bands = (*input_bands, self._mask_band)
        super().__init__(
            input_bands=input_bands,
            basis=basis_band or self.bands[0],
            **kwargs)

        if nodata_classes is not None:
            nodata_classes = tuple(nodata_classes)

        self.cloud_classes = tuple(cloud_classes)
        self.filters = filters

        self._renames = aux_names
        self.aux_bands = tuple(
            self._renames.get(k, k) for k in ("smad", "emad", "bcmad", "count")
        )

        self._work_chunks = work_chunks

    @property
    def measurements(self) -> Tuple[str, ...]:
        return self.bands + self.aux_bands

    def native_transform(self, xx: xr.Dataset) -> xr.Dataset:
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

    def input_data(self, datasets: Sequence[Dataset], geobox: GeoBox) -> xr.Dataset:
        erased = load_enum_filtered(
            datasets,
            self._mask_band,
            geobox,
            categories=self.cloud_classes,
            filters=self.filters,
            groupby=self.groupby,
            resampling=self.resampling,
            chunks={},
        )
        xx = super.input_data(datasets, geobox)
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


register("gm-generic", StatsGM)


class StatsGMS2(StatsGM):
    NAME = "gm_s2_annual"
    SHORT_NAME = NAME
    VERSION = "0.0.0"
    PRODUCT_FAMILY = "geomedian"

    def __init__(
        self,
        bands: Optional[Tuple[str, ...]] = None,
        mask_band: str = "SCL",
        cloud_classes: Tuple[str, ...] = (
            "cloud shadows",
            "cloud medium probability",
            "cloud high probability",
            "thin cirrus",
        ),
        filters: Optional[Iterable[Tuple[str, int]]] = [("opening", 2), ("dilation",5)],
        aux_names=dict(smad="SMAD", emad="EMAD", bcmad="BCMAD", count="COUNT"),
        rgb_bands=None,
        **kwargs
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

        super().__init__(
            bands=bands,
            mask_band=mask_band,
            cloud_classes=cloud_classes,
            filters=filters,
            aux_names=aux_names,
            rgb_bands=rgb_bands,
            **kwargs
        )


register("gm-s2", StatsGMS2)


class StatsGMLS(StatsGM):
    NAME = "gm_ls_annual"
    SHORT_NAME = NAME
    VERSION = "3.0.0"
    PRODUCT_FAMILY = "geomedian"

    def __init__(
        self,
        bands: Optional[Tuple[str, ...]] = None,
        mask_band: str = "fmask",
        cloud_classes: Tuple[str, ...] = ("cloud", "shadow"),
        nodata_classes: Optional[Tuple[str, ...]] = ("nodata",),
        aux_names=dict(smad="sdev", emad="edev", bcmad="bcdev", count="count"),
        rgb_bands=None,
        **kwargs
    ):
        if bands is None:
            bands = (
                "red",
                "green",
                "blue",
                "nir",
                "swir1",
                "swir2",
            )
            if rgb_bands is None:
                rgb_bands = ("red", "green", "blue")

        super().__init__(
            bands=bands,
            mask_band=mask_band,
            cloud_classes=cloud_classes,
            nodata_classes=nodata_classes,
            aux_names=aux_names,
            rgb_bands=rgb_bands,
            **kwargs,
        )


register("gm-ls", StatsGMLS)
