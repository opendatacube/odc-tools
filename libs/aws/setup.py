from setuptools import setup

setup(
    name='odc_aws',

    version='0.1',
    author='Open Data Cube',
    author_email='',
    maintainer='Open Data Cube',

    description='Various AWS helper methods',
    long_description='',

    license='Apache License 2.0',

    tests_require=['pytest'],
    install_requires=[
        'botocore',
    ],

    packages=['odc.aws'],
    zip_safe=False,
)
