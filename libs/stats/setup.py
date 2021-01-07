from setuptools import setup

setup(
    name="odc_stats",
    use_scm_version={"root": "../..", "relative_to": __file__},
    setup_requires=["setuptools_scm"],
    author="Open Data Cube",
    author_email="",
    maintainer="Open Data Cube",
    maintainer_email="",
    description="Statistical Product Generation Framework",
    long_description="",
    license="Apache License 2.0",
    tests_require=["pytest"],
    install_requires=[
        "datacube",
        "odc_index",
        "odc_dscache",
        "odc_algo",
        "odc_aws",
        "dataclasses",
        "tqdm",
    ],
    packages=["odc.stats"],
    zip_safe=False,
    entry_points={"console_scripts": ["odc-stats = odc.stats.cli:main"]},
)
