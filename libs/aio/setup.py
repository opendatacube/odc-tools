from setuptools import setup

setup(
    name='odc_aio',

    use_scm_version={"root": "../..", "relative_to": __file__},
    setup_requires=['setuptools_scm'],

    author='Open Data Cube',
    author_email='',
    maintainer='Open Data Cube',
    maintainer_email='',

    description='Async IO (from S3)',
    long_description='',

    license='Apache License 2.0',

    tests_require=['pytest'],
    install_requires=[
        'aiobotocore >= 1.0',
        'botocore',
        'odc_aws',
        'odc_ppt',
    ],

    packages=['odc.aio'],
    zip_safe=False,
)
