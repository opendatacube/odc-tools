from setuptools import setup

setup(
    name='odc_dtools',

    version='1',

    author='Kirill Kouzoubov',
    author_email='kirill.kouzoubov@ga.gov.au',

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
