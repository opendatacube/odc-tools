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
    install_requires=['distributed',
                      'rasterio'],

    packages=['odc.dtools'],
    zip_safe=False,
)
