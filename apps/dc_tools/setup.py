from setuptools import setup

setup(
    name='odc_apps_dc_tools',

    version='1',
    author='Open Data Cube',
    author_email='',
    maintainer='Open Data Cube',
    maintainer_email='',

    description='CLI utils for working datacube index',
    long_description='',
    license='Apache License 2.0',

    python_requires='>=3.5',
    tests_require=['pytest'],

    install_requires=[
        "click",
        'datacube',
        'odc_index @ git+https://github.com/opendatacube/dea-proto.git#egg=odc_index&subdirectory=libs/index',
        'odc_io @ git+https://github.com/opendatacube/dea-proto.git#egg=odc_io&subdirectory=libs/io',
    ],

    entry_points={
        'console_scripts': [
            'dc-index-from-tar = odc.apps.dc_tools.index_from_tar:cli',
            'dc-index-export-md = odc.apps.dc_tools.export_md:cli',
        ]
    },

    packages=['odc.apps.dc_tools'],
    zip_safe=False,
)
