from setuptools import setup

setup(
    name='odc_dscache',

    use_scm_version={"root": "../..", "relative_to": __file__},
    setup_requires=['setuptools_scm'],

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
        'odc-index',
        'zstandard',
        'lmdb',
        'click',
        'toolz',
    ],

    packages=[
        'odc.dscache',
        'odc.dscache.tools',
        'odc.dscache.apps',
    ],
    zip_safe=False,
    entry_points={
        'console_scripts': [
            'slurpy = odc.dscache.apps.slurpy:cli',
            'dstiler = odc.dscache.apps.dstiler:cli',
        ]
    }
)
