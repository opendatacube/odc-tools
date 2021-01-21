from setuptools import setup

setup(
    name="odc_apps_cloud",
    use_scm_version={"root": "../..", "relative_to": __file__},
    setup_requires=["setuptools_scm"],
    author="Open Data Cube",
    author_email="",
    maintainer="Open Data Cube",
    maintainer_email="",
    description="CLI utils for working with objects/files in the cloud",
    long_description="",
    license="Apache License 2.0",
    tests_require=["pytest"],
    install_requires=[
        "odc_aws",
        "odc_io",
        "odc_aio",
        "odc_ppt",
        "odc_thredds",
        "click",
    ],
    extras_require={
        "GCP": ["google-cloud-storage"],
        "THREDDS": ["thredds_crawler", "requests"],
    },
    entry_points={
        "console_scripts": [
            "thredds-to-tar = odc.apps.cloud.thredds_to_tar:cli [THREDDS]",
            "gs-to-tar = odc.apps.cloud.gs_to_tar:cli [GCP]",
            "s3-find = odc.apps.cloud.s3_find:cli",
            "s3-inventory-dump = odc.apps.cloud.s3_inventory:cli",
            "s3-to-tar = odc.apps.cloud.s3_to_tar:cli",
            "redrive-queue = odc.apps.cloud.redrive_to_queue:cli",
        ]
    },
    packages=["odc.apps.cloud"],
    zip_safe=False,
)
