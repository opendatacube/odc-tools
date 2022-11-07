import os

import configparser
import docker
import json
import psycopg2
import pytest
import time
import yaml
from pathlib import Path

from datacube import Datacube
from datacube.drivers.postgres import _core as pgres_core
from datacube.index import index_connect
from datacube.utils import documents
from odc.apps.dc_tools.add_update_products import add_update_products

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
def test_data_dir():
    return str(TEST_DATA_FOLDER)


@pytest.fixture
def aws_env(monkeypatch):
    monkeypatch.setenv("AWS_DEFAULT_REGION", "us-west-2")


@pytest.fixture
def usgs_landsat_stac():
    with TEST_DATA_FOLDER.joinpath(USGS_LANDSAT_STAC).open("r", encoding="utf8") as f:
        return json.load(f)


@pytest.fixture
def landsat_stac():
    with TEST_DATA_FOLDER.joinpath(LANDSAT_STAC).open("r", encoding="utf8") as f:
        metadata = json.load(f)
    return metadata


@pytest.fixture
def lidar_stac():
    with TEST_DATA_FOLDER.joinpath(LIDAR_STAC).open("r", encoding="utf8") as f:
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
    with TEST_DATA_FOLDER.joinpath(SENTINEL_STAC).open("r", encoding="utf8") as f:
        metadata = json.load(f)
    return metadata


@pytest.fixture
def sentinel_odc():
    with TEST_DATA_FOLDER.joinpath(SENTINEL_ODC).open("r", encoding="utf8") as f:
        metadata = json.load(f)
    return metadata


@pytest.fixture
def maturity_product_doc():
    with TEST_DATA_FOLDER.joinpath(MATURITY_PRODUCT).open("r", encoding="utf8") as f:
        doc = yaml.safe_load(f)
    return doc


@pytest.fixture
def nrt_dsid():
    return "2e9f4623-c51c-5233-869a-bb690f8c2cac"


@pytest.fixture
def final_dsid():
    return "9f27a15e-3cdf-4e3f-a58e-dd624b2c3bef"


GET_DB_FROM_ENV = "get-the-db-from-the-environment-variable"


@pytest.fixture(scope="session")
def postgresql_server():
    """
    Provide a temporary PostgreSQL server for the test session using Docker.
    :return: dictionary configuration required to connect to the server
    """

    # If we're running inside docker already, don't attempt to start a container!
    # Hopefully we're using the `with-test-db` script and can use *that* database.
    if Path("/.dockerenv").exists() and os.environ.get("DATACUBE_DB_URL"):
        yield GET_DB_FROM_ENV

    else:

        client = docker.from_env()
        container = client.containers.run(
            "postgres:alpine",
            auto_remove=True,
            remove=True,
            detach=True,
            environment={
                "POSTGRES_PASSWORD": "badpassword",
                "POSTGRES_USER": "odc_tools_test",
            },
            ports={"5432/tcp": None},
        )
        try:
            while not container.attrs["NetworkSettings"]["Ports"]:
                time.sleep(1)
                container.reload()
            host_port = container.attrs["NetworkSettings"]["Ports"]["5432/tcp"][0][
                "HostPort"
            ]
            # From the documentation for the postgres docker image. The value of POSTGRES_USER
            # is used for both the user and the default database.
            yield {
                "db_hostname": "127.0.0.1",
                "db_username": "odc_tools_test",
                "db_port": host_port,
                "db_database": "odc_tools_test",
                "db_password": "badpassword",
                "index_driver": "default",
            }
            # 'f"postgresql://odc_tools_test:badpassword@localhost:{host_port}/odc_tools_test",
        finally:
            container.remove(v=True, force=True)


@pytest.fixture
def odc_test_db(postgresql_server, tmp_path, monkeypatch):
    if postgresql_server == GET_DB_FROM_ENV:
        return os.environ["DATACUBE_DB_URL"]
    else:
        temp_datacube_config_file = tmp_path / "test_datacube.conf"

        config = configparser.ConfigParser()
        config["default"] = postgresql_server
        with open(temp_datacube_config_file, "w", encoding="utf8") as fout:
            config.write(fout)

        # This environment variable points to the configuration file, and is used by the odc-tools CLI apps
        # as well as direct ODC API access, eg creating `Datacube()`
        monkeypatch.setenv(
            "DATACUBE_CONFIG_PATH",
            str(temp_datacube_config_file.absolute()),
        )
        # This environment is used by the `datacube ...` CLI tools, which don't obey the same environment variables
        # as the API and odc-tools apps.
        # See https://github.com/opendatacube/datacube-core/issues/1258 for more
        # pylint:disable=consider-using-f-string
        postgres_url = "postgresql://{db_username}:{db_password}@{db_hostname}:{db_port}/{db_database}".format(
            **postgresql_server
        )
        monkeypatch.setenv("DATACUBE_DB_URL", postgres_url)
        while True:
            try:
                with psycopg2.connect(postgres_url):
                    break
            except psycopg2.OperationalError:
                print("Waiting for PostgreSQL to become available")
                time.sleep(1)
        return postgres_url


@pytest.fixture
def odc_db(odc_test_db):
    """
    Provide a temporary PostgreSQL server initialised by ODC, usable as
    the default ODC DB by setting environment variables.
    :param odc_test_db:
    :return: Datacube instance
    """

    index = index_connect(validate_connection=False)
    index.init_db()

    dc = Datacube(index=index)

    yield dc

    dc.close()
    pgres_core.drop_db(index._db._engine)  # pylint:disable=protected-access
    # We need to run this as well, I think because SQLAlchemy grabs them into it's MetaData,
    # and attempts to recreate them. WTF TODO FIX
    remove_postgres_dynamic_indexes()
    # with psycopg2.connect(odc_test_db) as conn:
    #     with conn.cursor() as cur:
    #         cur.execute("DROP SCHEMA IF EXISTS agdc CASCADE;")


def remove_postgres_dynamic_indexes():
    """
    Clear any dynamically created postgresql indexes from the schema.
    """
    # Our normal indexes start with "ix_", dynamic indexes with "dix_"
    for table in pgres_core.METADATA.tables.values():
        table.indexes.intersection_update(
            [i for i in table.indexes if not i.name.startswith("dix_")]
        )


@pytest.fixture
def odc_test_db_with_products(odc_db):
    local_csv = str(Path(__file__).parent / "data/example_product_list.csv")
    added, updated, failed = add_update_products(odc_db, local_csv)

    assert failed == 0


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
