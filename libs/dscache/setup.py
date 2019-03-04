from setuptools import setup

setup(
    name='odc_dscache',

    version='1',
    author='Open Data Cube',
    author_email='',
    maintainer='Open Data Cube',
    maintainer_email='',

    description='ODC Dataset File Cache',
    long_description='',

    license='Apache License 2.0',

    tests_require=['pytest'],
    install_requires=[
        'datacube',
        'zstandard',
        'lmdb',
        'click',
        'toolz',
    ],

    packages=['odc.dscache'],
    zip_safe=False,
    entry_points={
        'console_scripts': [
            'slurpy = odc.dscache.apps.slurpy:cli',
            'dstiler = odc.dscache.apps.dstiler:cli',
        ]
    }
)
