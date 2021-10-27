from datetime import datetime, timedelta
from types import SimpleNamespace
from copy import deepcopy

import pystac
import pytest
from datacube import Datacube
from datacube.model import Dataset, DatasetType, metadata_from_doc
from datacube.index.eo3 import prep_eo3
from datacube.index.index import default_metadata_type_docs
from odc.stats.model import DateTimeRange
from odc.stats.tasks import TaskReader
from odc.stats.utils import (
    bin_annual,
    bin_full_history,
    bin_generic,
    bin_seasonal,
    mk_season_rules,
    season_binner,
    fuse_products,
    fuse_ds,
    mk_wo_season_rules
)

from . import gen_compressed_dss


def test_stac(test_db_path):
    from odc.stats.model import product_for_plugin
    from odc.stats.plugins.gm import StatsGMS2

    product = product_for_plugin(StatsGMS2(), location="/tmp/")
    reader = TaskReader(test_db_path, product)
    task = reader.load_task(reader.all_tiles[0])

    stac_meta = task.render_metadata()

    stac_item = pystac.Item.from_dict(stac_meta)
    stac_item.validate()


def test_binning():
    dss = list(gen_compressed_dss(100, dt0=datetime(2000, 1, 1), step=27))
    cells = {
        (0, 1): SimpleNamespace(
            dss=dss, geobox=None, idx=None, utc_offset=timedelta(seconds=0)
        )
    }

    def verify(task):
        for (t, *_), dss in task.items():
            period = DateTimeRange(t)
            assert all(ds.time in period for ds in dss)

    start = min(ds.time for ds in dss)
    end = max(ds.time for ds in dss)

    tasks_y = bin_annual(cells)
    verify(tasks_y)

    tasks_fh = bin_full_history(cells, start, end)
    verify(tasks_fh)

    yy_bins = [DateTimeRange(t) for t, *_ in tasks_y]

    tasks = bin_generic(cells, yy_bins)
    verify(tasks)
    for t in yy_bins:
        k = (t.short, 0, 1)
        dss1 = tasks[k]
        dss2 = tasks_y[k]

        assert set(ds.id for ds in dss1) == set(ds.id for ds in dss2)

    tasks = bin_seasonal(cells, 6, 1)
    verify(tasks)

def test_wo_season_binner():
    wo_apr_to_oct_seasons_rules = {
        4: "04--P7M",
        5: "04--P7M",
        6: "04--P7M",
        7: "04--P7M",
        8: "04--P7M",
        9: "04--P7M",
        10: "04--P7M",
    }

    # start from April, length is 7 month
    assert mk_wo_season_rules(7, anchor=4) == wo_apr_to_oct_seasons_rules

    wo_nov_to_mar_seasons_rules = {
        11: "11--P5M",
        12: "11--P5M",
        1: "11--P5M",
        2: "11--P5M",
        3: "11--P5M",
    }

    # start from Nov, length is 5 month
    assert mk_wo_season_rules(5, anchor=11) == wo_nov_to_mar_seasons_rules

    binner = season_binner(wo_apr_to_oct_seasons_rules)
    assert binner(datetime(2020, 1, 28)) == ""
    assert binner(datetime(2020, 2, 21)) == ""
    assert binner(datetime(2020, 3, 1)) == ""
    assert binner(datetime(2020, 4, 1)) == "2020-04--P7M"
    assert binner(datetime(2020, 5, 31)) == "2020-04--P7M"
    assert binner(datetime(2020, 10, 31)) == "2020-04--P7M"
    assert binner(datetime(2020, 12, 30)) == ""

    binner = season_binner(wo_nov_to_mar_seasons_rules)
    assert binner(datetime(2020, 1, 28)) == "2019-11--P5M"
    assert binner(datetime(2020, 2, 21)) == "2019-11--P5M"
    assert binner(datetime(2020, 3, 1)) == "2019-11--P5M"
    assert binner(datetime(2020, 4, 1)) == ""
    assert binner(datetime(2020, 5, 31)) == ""
    assert binner(datetime(2020, 10, 31)) == ""
    assert binner(datetime(2020, 12, 30)) == "2020-11--P5M"

def test_season_binner():
    four_seasons_rules = {
        12: "12--P3M",
        1: "12--P3M",
        2: "12--P3M",
        3: "03--P3M",
        4: "03--P3M",
        5: "03--P3M",
        6: "06--P3M",
        7: "06--P3M",
        8: "06--P3M",
        9: "09--P3M",
        10: "09--P3M",
        11: "09--P3M",
    }

    assert mk_season_rules(3, anchor=12) == four_seasons_rules

    binner = season_binner(four_seasons_rules)
    assert binner(datetime(2020, 1, 28)) == "2019-12--P3M"
    assert binner(datetime(2020, 2, 21)) == "2019-12--P3M"
    assert binner(datetime(2020, 3, 1)) == "2020-03--P3M"
    assert binner(datetime(2020, 4, 1)) == "2020-03--P3M"
    assert binner(datetime(2020, 5, 31)) == "2020-03--P3M"
    assert binner(datetime(2020, 6, 1)) == "2020-06--P3M"
    assert binner(datetime(2020, 12, 30)) == "2020-12--P3M"

    binner = season_binner({})
    for m in range(1, 13):
        assert binner(datetime(2003, m, 10)) == ""

    binner = season_binner({1: "01--P1M"})
    assert binner(datetime(2001, 10, 19)) == ""
    assert binner(datetime(2001, 1, 19)) == "2001-01--P1M"


@pytest.mark.parametrize(
    "months,anchor",
    [(6, 1)]
)
def test_bin_seasonal_mk_season_rules(months, anchor):
    season_rules = mk_season_rules(months, anchor)
    assert {'01--P6M', '07--P6M'} == set(season_rules.values())


@pytest.fixture
def fc_definition():
    definition = {
        'name': 'ga_ls_fc_3',
        'metadata': {
            'product': {'name': 'ga_ls_fc_3'},
            'properties': {'odc:file_format': 'GeoTIFF', 'odc:product_family': 'fc'}
        },
        'description': 'Geoscience Australia Landsat Fractional Cover Collection 3',
        'measurements': [
            {'name': 'bs', 'dtype': 'uint8', 'units': 'percent', 'nodata': 255, 'aliases': ['bare']},
            {'name': 'pv', 'dtype': 'uint8', 'units': 'percent', 'nodata': 255, 'aliases': ['green_veg']},
            {'name': 'npv', 'dtype': 'uint8', 'units': 'percent', 'nodata': 255, 'aliases': ['dead_veg']},
            {'name': 'ue', 'dtype': 'uint8', 'units': '1', 'nodata': 255, 'aliases': ['err']}],
        'metadata_type': 'eo3'
    }
    return definition


@pytest.fixture
def wo_definition():
    definition = {
        'name': 'ga_ls_wo_3',
        'metadata': {
            'product': {'name': 'ga_ls_wo_3'},
            'properties': {'odc:file_format': 'GeoTIFF', 'odc:product_family': 'wo'}
        },
        'description': 'Geoscience Australia Landsat Water Observations Collection 3',
        'measurements': [{'name': 'water', 'dtype': 'uint8', 'units': '1', 'nodata': 1}],
        'metadata_type': 'eo3'
    }
    return definition


@pytest.fixture
def usgs_ls8_sr_definition():
    definition = {
        'name': 'ls8_sr',
        'description': 'USGS Landsat 8 Collection 2 Level-2 Surface Reflectance',
        'metadata_type': 'eo3',
        'measurements': [
            {
                'name': 'QA_PIXEL',
                'dtype': 'uint16',
                'units': 'bit_index',
                'nodata': '1',
                'flags_definition': {
                    'snow': {'bits': 5, 'values': {'0': 'not_high_confidence', '1': 'high_confidence'}},
                    'clear': {'bits': 6, 'values': {'0': False, '1': True}},
                    'cloud': {'bits': 3, 'values': {'0': 'not_high_confidence', '1': 'high_confidence'}},
                    'water': {'bits': 7, 'values': {'0': 'land_or_cloud', '1': 'water'}},
                    'cirrus': {'bits': 2, 'values': {'0': 'not_high_confidence', '1': 'high_confidence'}},
                    'nodata': {'bits': 0, 'values': {'0': False, '1': True}},
                    'cloud_shadow': {'bits': 4, 'values': {'0': 'not_high_confidence', '1': 'high_confidence'}},
                    'dilated_cloud': {'bits': 1, 'values': {'0': 'not_dilated', '1': 'dilated'}},
                    'cloud_confidence': {'bits': [8, 9], 'values': {'0': 'none', '1': 'low', '2': 'medium', '3': 'high'}},
                    'cirrus_confidence': {'bits': [14, 15], 'values': {'0': 'none', '1': 'low', '2': 'reserved', '3': 'high'}},
                    'snow_ice_confidence': {'bits': [12, 13], 'values': {'0': 'none', '1': 'low', '2': 'reserved', '3': 'high'}},
                    'cloud_shadow_confidence': {'bits': [10, 11], 'values': {'0': 'none', '1': 'low', '2': 'reserved', '3': 'high'}}
                }
            }
        ]
    }
    return definition


@pytest.fixture
def dc():
    return Datacube()


def test_fuse_products(wo_definition, fc_definition):
    standard_metadata_types = {d["name"]: metadata_from_doc(d) for d in default_metadata_type_docs()}
    eo3 = standard_metadata_types["eo3"]

    wo_product = DatasetType(eo3, wo_definition)
    fc_product = DatasetType(eo3, fc_definition)
    fuse_products(wo_product, fc_product)

    bad_definition = deepcopy(wo_definition)
    bad_definition["metadata"]["properties"]["odc:file_format"] = "bad"
    bad_product = DatasetType(eo3, bad_definition)
    with pytest.raises(ValueError):
        fuse_products(bad_product, fc_product)

    bad_definition = deepcopy(wo_definition)
    bad_definition["measurements"].append(fc_definition["measurements"][1])
    bad_product = DatasetType(eo3, bad_definition)
    with pytest.raises(ValueError):
        fuse_products(bad_product, fc_product)

    # Test fusing without file_format on the product
    wo_no_ff = deepcopy(wo_definition)
    del wo_no_ff["metadata"]["properties"]["odc:file_format"]
    fc_no_ff = deepcopy(fc_definition)
    del fc_no_ff["metadata"]["properties"]["odc:file_format"]

    wo_product = DatasetType(eo3, wo_no_ff)
    fc_product = DatasetType(eo3, fc_no_ff)
    fuse_products(wo_product, fc_product)


def _get_msr_paths(ds):
    return set(m["path"] for m in ds.metadata_doc["measurements"].values())


def test_fuse_dss(wo_definition, fc_definition):
    standard_metadata_types = {d["name"]: metadata_from_doc(d) for d in default_metadata_type_docs()}
    eo3 = standard_metadata_types["eo3"]

    wo_product = DatasetType(eo3, wo_definition)
    fc_product = DatasetType(eo3, fc_definition)
    fused_product = fuse_products(wo_product, fc_product)

    wo_metadata = {
        'id': 'e9fb6737-b93d-5cd9-bfe6-7e634abc9905',
        'crs': 'epsg:32655',
        'grids': {'default': {'shape': [7211, 8311], 'transform': [30.0, 0.0, 423285.0, 0.0, -30.0, -4040385.0, 0.0, 0.0, 1.0]}},
        'label': 'ga_ls_wo_3_091086_2020-04-04_final',
        '$schema': 'https://schemas.opendatacube.org/dataset',
        'lineage': {'source_datasets': {}},
        'product': {'name': 'ga_ls_wo_3'},
        'properties': {
            'title': 'ga_ls_wo_3_091086_2020-04-04_final',
            'eo:gsd': 30.0,
            'created': '2021-03-09T23:22:42.130266Z',
            'datetime': '2020-04-04T23:33:10.644420Z',
            'proj:epsg': 32655,
            'proj:shape': [7211, 8311],
            'eo:platform': 'landsat-7',
            'odc:product': 'ga_ls_wo_3',
            'odc:producer': 'ga.gov.au',
            'eo:instrument': 'ETM',
            'eo:cloud_cover': 44.870310145260326,
            'eo:sun_azimuth': 49.20198554,
            'proj:transform': [30.0, 0.0, 423285.0, 0.0, -30.0, -4040385.0, 0.0, 0.0, 1.0],
            'landsat:wrs_row': 86,
            'odc:file_format': 'GeoTIFF',
            'odc:region_code': '091086',
            'dtr:end_datetime': '2020-04-04T23:33:24.461679Z',
            'eo:sun_elevation': 32.7056476,
            'landsat:wrs_path': 91,
            'dtr:start_datetime': '2020-04-04T23:32:56.662365Z',
            'odc:product_family': 'wo',
            'odc:dataset_version': '1.6.0',
            'dea:dataset_maturity': 'final',
            'odc:collection_number': 3,
            'odc:naming_conventions': 'dea_c3',
            'odc:processing_datetime': '2020-04-04T23:33:10.644420Z',
            'landsat:landsat_scene_id': 'LE70910862020095ASA00',
            'landsat:collection_number': 1,
            'landsat:landsat_product_id': 'LE07_L1TP_091086_20200404_20200501_01_T1',
            'landsat:collection_category': 'T1'},
            'measurements': {'water': {'path': 'ga_ls_wo_3_091086_2020-04-04_final_water.tif'}
        }
    }

    fc_metadata = {
        'id': '41980746-4f17-5e0c-86a0-92cca8d3c99d',
        'crs': 'epsg:32655',
        'grids': {'default': {'shape': [7211, 8311], 'transform': [30.0, 0.0, 423285.0, 0.0, -30.0, -4040385.0, 0.0, 0.0, 1.0]}},
        'label': 'ga_ls_fc_3_091086_2020-04-04_final',
        '$schema': 'https://schemas.opendatacube.org/dataset',
        'product': {'name': 'ga_ls_fc_3'},
        'properties': {
            'title': 'ga_ls_fc_3_091086_2020-04-04_final',
            'eo:gsd': 30.0,
            'created': '2021-03-10T04:14:49.645196Z',
            'datetime': '2020-04-04T23:33:10.644420Z',
            'proj:epsg': 32655,
            'proj:shape': [7211, 8311],
            'eo:platform': 'landsat-7',
            'odc:product': 'ga_ls_fc_3',
            'odc:producer': 'ga.gov.au',
            'eo:instrument': 'ETM',
            'eo:cloud_cover': 44.870310145260326,
            'eo:sun_azimuth': 49.20198554,
            'proj:transform': [30.0, 0.0, 423285.0, 0.0, -30.0, -4040385.0, 0.0, 0.0, 1.0],
            'landsat:wrs_row': 86,
            'odc:file_format': 'GeoTIFF',
            'odc:region_code': '091086',
            'dtr:end_datetime': '2020-04-04T23:33:24.461679Z',
            'eo:sun_elevation': 32.7056476,
            'landsat:wrs_path': 91,
            'dtr:start_datetime': '2020-04-04T23:32:56.662365Z',
            'odc:product_family': 'fc',
            'odc:dataset_version': '2.5.0',
            'dea:dataset_maturity': 'final',
            'odc:collection_number': 3,
            'odc:naming_conventions': 'dea_c3',
            'odc:processing_datetime': '2020-04-04T23:33:10.644420Z',
            'landsat:landsat_scene_id': 'LE70910862020095ASA00',
            'landsat:collection_number': 1,
            'landsat:landsat_product_id': 'LE07_L1TP_091086_20200404_20200501_01_T1',
            'landsat:collection_category': 'T1'},
            'measurements': {'bs': {'path': 'ga_ls_fc_3_091086_2020-04-04_final_bs.tif'},
            'pv': {'path': 'ga_ls_fc_3_091086_2020-04-04_final_pv.tif'},
            'ue': {'path': 'ga_ls_fc_3_091086_2020-04-04_final_ue.tif'},
            'npv': {'path': 'ga_ls_fc_3_091086_2020-04-04_final_npv.tif'}
        }
    }

    # paths get made absolute here
    # TODO: force paths to stay relative
    wo_uris = ["s3://dea-public-data/derivative/ga_ls_wo_3/1-6-0/091/086/2020/04/04/ga_ls_wo_3_091086_2020-04-04_final.stac-item.json"]
    wo_ds = Dataset(wo_product, prep_eo3(wo_metadata), uris=wo_uris)
    fc_uris = ["s3://dea-public-data/derivative/ga_ls_fc_3/2-5-0/091/086/2020/04/04/ga_ls_fc_3_091086_2020-04-04_final.stac-item.json"]
    fc_ds = Dataset(fc_product, prep_eo3(fc_metadata), uris=fc_uris)

    fused_ds = fuse_ds(wo_ds, fc_ds, fused_product)
    assert _get_msr_paths(fused_ds) == _get_msr_paths(fc_ds).union(_get_msr_paths(wo_ds))
    fused_ds = fuse_ds(wo_ds, fc_ds)
    assert _get_msr_paths(fused_ds) == _get_msr_paths(fc_ds).union(_get_msr_paths(wo_ds))

    bad_metadata = deepcopy(fc_metadata)
    bad_metadata["properties"]["datetime"] = '2020-04-03T23:33:10.644420Z'
    bad_ds = Dataset(fc_product, prep_eo3(bad_metadata), uris=fc_uris)
    with pytest.raises(ValueError):
        fused_ds = fuse_ds(wo_ds, bad_ds, fused_product)

    bad_metadata = deepcopy(fc_metadata)
    bad_metadata["crs"] = "epsg:32656"
    bad_ds = Dataset(fc_product, prep_eo3(bad_metadata), uris=fc_uris)
    with pytest.raises(ValueError):
        fused_ds = fuse_ds(wo_ds, bad_ds, fused_product)

    bad_metadata = deepcopy(fc_metadata)
    bad_metadata['grids']['default']['shape'] = [7212, 8311]
    bad_ds = Dataset(fc_product, prep_eo3(bad_metadata), uris=fc_uris)
    with pytest.raises(ValueError):
        fused_ds = fuse_ds(wo_ds, bad_ds, fused_product)

    bad_metadata = deepcopy(fc_metadata)
    bad_metadata['label'] += 'a'
    bad_ds = Dataset(fc_product, prep_eo3(bad_metadata), uris=fc_uris)
    with pytest.raises(ValueError):
        fused_ds = fuse_ds(wo_ds, bad_ds, fused_product)

    # Test fuse without odc:file_format
    wo_no_ff = deepcopy(wo_metadata)
    fc_no_ff = deepcopy(fc_metadata)
    del wo_no_ff["properties"]["odc:file_format"]
    del fc_no_ff["properties"]["odc:file_format"]
    wo_ds = Dataset(wo_product, prep_eo3(wo_no_ff), uris=wo_uris)
    fc_ds = Dataset(fc_product, prep_eo3(fc_no_ff), uris=fc_uris)
    fuse_ds(wo_ds, fc_ds, fused_product)
