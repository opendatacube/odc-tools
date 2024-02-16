import configparser
import json
import os
import time
from pathlib import Path

import boto3
import psycopg2
import pytest
import yaml
from click.testing import CliRunner
from datacube import Datacube
from datacube.drivers.postgres import _core as pgres_core
from datacube.index import index_connect
from datacube.model import MetadataType
from datacube.utils import documents
from moto import mock_aws
from moto.server import ThreadedMotoServer
from odc.apps.dc_tools.add_update_products import add_update_products

import docker

TEST_DATA_FOLDER: Path = Path(__file__).parent.joinpath("data")
LANDSAT_STAC: str = "ga_ls8c_ard_3-1-0_088080_2020-05-25_final.stac-item.json"
LANDSAT_ODC: str = "ga_ls8c_ard_3-1-0_088080_2020-05-25_final.odc-metadata.yaml"
LANDSAT_ODC_NRT: str = "ga_ls8c_ard_3-1-0_088080_2020-05-25_nrt.odc-metadata.yaml"
SENTINEL_STAC_OLD: str = "S2A_28QCH_20200714_0_L2A_old.json"
SENTINEL_ODC: str = "S2A_28QCH_20200714_0_L2A.odc-metadata.json"
USGS_LANDSAT_STAC: str = "LC08_L2SR_081119_20200101_20200823_02_T2.json"
LIDAR_STAC: str = "lidar_dem.json"
MATURITY_PRODUCT: str = "ga_ls5t_gm_product.yaml"
ESRI_LULC_STAC: str = "29V-2021.stac-item.json"
WORLD_WRAPPING_STAC: str = "world-wrapping.stac-item.json"


@pytest.fixture
def test_data_dir():
    return str(TEST_DATA_FOLDER)


@pytest.fixture
def aws_env(monkeypatch):
    monkeypatch.setenv("AWS_DEFAULT_REGION", "us-west-2")


@pytest.fixture
def mocked_aws_s3_env():
    """
    Run a Fake Local S3 Service on http://localhost:5000 and redirect odc.aio
    to use it via an env variable.
    """

    server = ThreadedMotoServer()
    server.start()
    # run tests
    os.environ["AWS_S3_ENDPOINT"] = "http://localhost:5000"
    os.environ["AWS_ACCESS_KEY_ID"] = "testing"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
    os.environ["AWS_SECURITY_TOKEN"] = "testing"
    os.environ["AWS_SESSION_TOKEN"] = "testing"
    os.environ["AWS_DEFAULT_REGION"] = "us-east-1"
    yield boto3.resource("s3", endpoint_url="http://localhost:5000")
    del os.environ["AWS_S3_ENDPOINT"]
    server.stop()


@pytest.fixture
def mocked_s3_datasets(mocked_aws_s3_env):
    with mock_aws():
        bucket = mocked_aws_s3_env.Bucket("odc-tools-test")
        bucket.create(
            ACL="public-read",
        )
        bucket.upload_file(
            Filename=str(TEST_DATA_FOLDER / "S2B_31QGB_20200831_0_L2A.json"),
            Key="sentinel-s2-l2a-cogs/31/Q/GB/2020/8/S2B_31QGB_20200831_0_L2A/"
            "S2B_31QGB_20200831_0_L2A.json",
        )
        bucket.upload_file(
            Filename=str(
                TEST_DATA_FOLDER
                / "ga_ls5t_nbart_gm_cyear_3_x30y14_1999--P1Y_final.stac-item.json"
            ),
            Key="ga_ls5t_nbart_gm_cyear_3/3-0-0/x30/y14/"
            "ga_ls5t_nbart_gm_cyear_3_x30y14_1999--P1Y_final.stac-item.json",
        )
        bucket.upload_file(
            Filename=str(
                TEST_DATA_FOLDER
                / "ga_s2am_ard_3-2-1_49JFM_2016-12-14_final.stac-item.json"
            ),
            Key="baseline/ga_s2am_ard_3/49/JFM/2016/12/14/20161214T092514/"
            "ga_s2am_ard_3-2-1_49JFM_2016-12-14_final.stac-item.json",
        )
        bucket.upload_file(
            Filename=str(
                TEST_DATA_FOLDER
                / "ga_s2am_ard_3-2-1_49JFM_2016-12-14_final.odc-metadata.yaml"
            ),
            Key="baseline/ga_s2am_ard_3/49/JFM/2016/12/14/20161214T092514/"
            "ga_s2am_ard_3-2-1_49JFM_2016-12-14_final.odc-metadata.yaml",
        )
        test_datasets = list((TEST_DATA_FOLDER / "cemp_insar").glob("**/*.yaml"))
        test_datasets.extend((TEST_DATA_FOLDER / "derivative").glob("**/*.yaml"))
        for fname in test_datasets:
            bucket.upload_file(
                Filename=str(fname.absolute()),
                Key=str(fname.relative_to(TEST_DATA_FOLDER)),
            )
        yield bucket


@pytest.fixture
def world_wrapping_stac():
    with TEST_DATA_FOLDER.joinpath(WORLD_WRAPPING_STAC).open("r", encoding="utf8") as f:
        return json.load(f)


@pytest.fixture
def esri_lulc_stac():
    with TEST_DATA_FOLDER.joinpath(ESRI_LULC_STAC).open("r", encoding="utf8") as f:
        return json.load(f)


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
def sentinel_stac_old():
    """Return an example old version of the Sentinel 2 L2A STAC metadata."""
    with TEST_DATA_FOLDER.joinpath(SENTINEL_STAC_OLD).open("r", encoding="utf8") as f:
        metadata = json.load(f)
    return metadata


@pytest.fixture
def sentinel_odc():
    """Return an example ODC EO3 metadata dict. Pair with `sentinel_stac_odc`"""
    with TEST_DATA_FOLDER.joinpath(SENTINEL_ODC).open("r", encoding="utf8") as f:
        metadata = json.load(f)
    return metadata


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
def odc_test_db(
    postgresql_server, tmp_path, monkeypatch
):  # pytest: disable=inconsistent-return-statements
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

    with open(
        TEST_DATA_FOLDER / "eo3_sentinel_ard.odc-type.yaml", encoding="utf8"
    ) as f:
        meta_doc = yaml.safe_load(f)
        dc.index.metadata_types.add(MetadataType(meta_doc))
    with open(TEST_DATA_FOLDER / MATURITY_PRODUCT, encoding="utf8") as f:
        doc = yaml.safe_load(f)
        dc.index.products.add_document(doc)

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
def odc_test_db_with_products(odc_db: Datacube):
    local_csv = str(Path(__file__).parent / "data/example_product_list.csv")
    added, updated, failed = add_update_products(odc_db, local_csv)

    assert failed == 0
    yield odc_db


@pytest.fixture
def ls5t_dsid():
    return "57814bc4-6fdf-4fa1-84e5-865b364c4284"


@pytest.fixture
def s2am_dsid():
    return "e2baf679-c20a-479f-86c5-ffd98c65ff87"


@pytest.fixture
def odc_db_for_archive(odc_test_db_with_products: Datacube):
    """Create a temporary test database with some pre-indexed datasets."""
    # pylint:disable=import-outside-toplevel
    from odc.apps.dc_tools.fs_to_dc import cli as fs_to_dc_cli

    for filename in (
        "ga_ls5t_nbart_gm_cyear_3_x30y14_1999--P1Y_final.stac-item.json",
        "ga_s2am_ard_3-2-1_49JFM_2016-12-14_final.stac-item.json",
    ):
        result = CliRunner().invoke(
            fs_to_dc_cli, ["--stac", "--glob", filename, str(TEST_DATA_FOLDER)]
        )
        print(result.output)
        assert result.exit_code == 0

    return odc_test_db_with_products
