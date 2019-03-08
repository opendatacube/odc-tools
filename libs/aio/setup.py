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
    install_requires=[
        'aiobotocore',
        'botocore',
        'odc_aws @ git+https://github.com/opendatacube/dea-proto.git#egg=odc_aws&subdirectory=libs/aws',
        'odc_ppt @ git+https://github.com/opendatacube/dea-proto.git#egg=odc_ppt&subdirectory=libs/ppt',
    ],

    packages=['odc.aio'],
    zip_safe=False,
)
