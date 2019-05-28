from setuptools import setup

setup(
    name='odc_geom',

    version='1',
    author='Open Data Cube',
    author_email='',
    maintainer='Open Data Cube',

    description='Miscellaneous Geometry helper methods',
    long_description='',

    license='Apache License 2.0',

    tests_require=['pytest'],
    install_requires=[
        'affine',
    ],

    packages=['odc.geom'],
    zip_safe=False,
)
