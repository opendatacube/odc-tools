from setuptools import setup

TEST_REQUIREMENTS = ["pytest", "deepdiff", "pytest-depends"]

setup(
    name="odc_apps_dc_tools",
    use_scm_version={"root": "../..", "relative_to": __file__},
    setup_requires=["setuptools_scm"],
    author="Open Data Cube",
    author_email="",
    maintainer="Open Data Cube",
    maintainer_email="",
    description="CLI utils for working with a datacube index",
    long_description="",
    license="Apache License 2.0",
    python_requires=">=3.5",
    tests_require=TEST_REQUIREMENTS,
    install_requires=[
        "click",
        "fsspec",
        "pystac-client",
        "toolz",
        "pyyaml",
        "datacube",
        "odc_index",
        "odc_io",
        "odc_aws",
        "odc_aio",
        "pystac",
    ],
    extras_require={
        "tests": TEST_REQUIREMENTS,
        "AZURE": ["odc_azure"],
        "THREDDS": ["odc_thredds"],
    },
    entry_points={
        "console_scripts": [
            "dc-sync-products = odc.apps.dc_tools.add_update_products:cli",
            "dc-index-from-tar = odc.apps.dc_tools.index_from_tar:cli",
            "dc-index-export-md = odc.apps.dc_tools.export_md:cli",
            "s3-to-dc = odc.apps.dc_tools.s3_to_dc:cli",
            "thredds-to-dc = odc.apps.dc_tools.thredds_to_dc:cli [THREDDS]",
            "sqs-to-dc = odc.apps.dc_tools.sqs_to_dc:cli",
            "stac-to-dc = odc.apps.dc_tools.stac_api_to_dc:cli",
            "azure-to-dc = odc.apps.dc_tools.azure_to_dc:cli [AZURE]",
            "esri-lc-to-dc = odc.apps.dc_tools.esri_land_cover_to_dc:cli",
        ]
    },
    packages=["odc.apps.dc_tools"],
    zip_safe=False,
    include_package_data=True,
    package_data={"": ["*.txt"]},
)
