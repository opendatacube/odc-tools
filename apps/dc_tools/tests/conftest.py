import json
from pathlib import Path

import pytest
import yaml
from datacube import Datacube
from datacube.utils import documents

TEST_DATA_FOLDER: Path = Path(__file__).parent.joinpath("data")
LANDSAT_STAC: str = "ga_ls8c_ard_3-1-0_088080_2020-05-25_final.stac-item.json"
LANDSAT_ODC: str = "ga_ls8c_ard_3-1-0_088080_2020-05-25_final.odc-metadata.yaml"
LANDSAT_ODC_NRT: str = "ga_ls8c_ard_3-1-0_088080_2020-05-25_nrt.odc-metadata.yaml"
SENTINEL_STAC: str = "S2A_28QCH_20200714_0_L2A.json"
SENTINEL_ODC: str = "S2A_28QCH_20200714_0_L2A.odc-metadata.json"
USGS_LANDSAT_STAC: str = "LC08_L2SR_081119_20200101_20200823_02_T2.json"
LIDAR_STAC: str = "lidar_dem.json"
MATURITY_PRODUCT: str = "ga_ls5t_gm_product.yaml"


@pytest.fixture
def aws_env(monkeypatch):
    monkeypatch.setenv("AWS_DEFAULT_REGION", "us-west-2")


@pytest.fixture
def usgs_landsat_stac():
    with TEST_DATA_FOLDER.joinpath(USGS_LANDSAT_STAC).open("r") as f:
        return json.load(f)


@pytest.fixture
def landsat_stac():
    with TEST_DATA_FOLDER.joinpath(LANDSAT_STAC).open("r") as f:
        metadata = json.load(f)
    return metadata


@pytest.fixture
def lidar_stac():
    with TEST_DATA_FOLDER.joinpath(LIDAR_STAC).open("r") as f:
        metadata = json.load(f)
    return metadata


@pytest.fixture
def landsat_odc():
    metadata = yield from documents.load_documents(
        TEST_DATA_FOLDER.joinpath(LANDSAT_ODC)
    )
    return metadata


@pytest.fixture
def sentinel_stac():
    with TEST_DATA_FOLDER.joinpath(SENTINEL_STAC).open("r") as f:
        metadata = json.load(f)
    return metadata


@pytest.fixture
def sentinel_odc():
    with TEST_DATA_FOLDER.joinpath(SENTINEL_ODC).open("r") as f:
        metadata = json.load(f)
    return metadata


@pytest.fixture
def maturity_product_doc():
    with TEST_DATA_FOLDER.joinpath(MATURITY_PRODUCT).open("r") as f:
        doc = yaml.safe_load(f)
    return doc


@pytest.fixture
def nrt_dsid():
    return "2e9f4623-c51c-5233-869a-bb690f8c2cac"


@pytest.fixture
def final_dsid():
    return "9f27a15e-3cdf-4e3f-a58e-dd624b2c3bef"


@pytest.fixture
def odc_db():
    try:
        return Datacube()
    except Exception:
        return None


@pytest.fixture
def odc_db_for_maturity_tests(odc_db, maturity_product_doc, nrt_dsid, final_dsid):
    if odc_db is None:
        return None
    # Ensure product present
    odc_db.index.products.add_document(maturity_product_doc)
    have_nrt, have_final = odc_db.index.datasets.bulk_has([nrt_dsid, final_dsid])
    # Ensure datasets absent
    for_deletion = []
    if have_nrt:
        for_deletion.append(nrt_dsid)
    if have_final:
        for_deletion.append(final_dsid)
    if for_deletion:
        odc_db.index.datasets.archive(for_deletion)
        odc_db.index.datasets.purge(for_deletion)
    return odc_db


@pytest.fixture
def ls5t_dsid():
    return "57814bc4-6fdf-4fa1-84e5-865b364c4284"


@pytest.fixture
def s2am_dsid():
    return "e2baf679-c20a-479f-86c5-ffd98c65ff87"


@pytest.fixture
def odc_db_for_sns(odc_db, ls5t_dsid, s2am_dsid):
    # remove s2am and ls5t datasets that will be added
    if odc_db is None:
        return None
    has_ls5t, has_s2am = odc_db.index.datasets.bulk_has([ls5t_dsid, s2am_dsid])
    for_deletion = []
    if has_ls5t:
        for_deletion.append(ls5t_dsid)
    if has_s2am:
        for_deletion.append(s2am_dsid)
    if for_deletion:
        odc_db.index.datasets.archive(for_deletion)
        odc_db.index.datasets.purge(for_deletion)
    return odc_db


@pytest.fixture
def odc_db_for_archive(odc_db, ls5t_dsid):
    # ls5t dataset must be present for it to be archived
    if odc_db is None:
        return None
    if not odc_db.index.datasets.get(ls5t_dsid):
        odc_db.index.datasets.add(ls5t_dsid)
    return odc_db
