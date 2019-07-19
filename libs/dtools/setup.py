from setuptools import setup

setup(
    name='odc_dtools',

    use_scm_version={"root": "../..", "relative_to": __file__},
    setup_requires=['setuptools_scm'],

    author='Open Data Cube',
    author_email='',
    maintainer='Open Data Cube',

    description='Miscellaneous dask.distributed helper methods',
    long_description='',

    license='Apache License 2.0',

    tests_require=['pytest'],
    install_requires=[
        'distributed',
        'rasterio',
        'odc_aws @ git+https://github.com/opendatacube/dea-proto.git#egg=odc_aws&subdirectory=libs/aws',
    ],

    packages=['odc.dtools'],
    zip_safe=False,
)
