from setuptools import setup

setup(
    name='odc_apps_dc_tools',

    use_scm_version={"root": "../..", "relative_to": __file__},
    setup_requires=['setuptools_scm'],

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
        'odc_index',
        'odc_io',
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
