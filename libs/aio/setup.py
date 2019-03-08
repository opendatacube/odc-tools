from setuptools import setup

setup(
    name='odc_aio',

    version='1',
    author='Open Data Cube',
    author_email='',
    maintainer='Open Data Cube',
    maintainer_email='',

    description='Async IO (from S3)',
    long_description='',

    license='Apache License 2.0',

    tests_require=['pytest'],
    install_requires=[],

    packages=['odc.aio'],
    zip_safe=False,
)
