from setuptools import setup

setup(
    name='odc_ui',

    use_scm_version={"root": "../..", "relative_to": __file__},
    setup_requires=['setuptools_scm'],

    author='Open Data Cube',
    author_email='',
    maintainer='Open Data Cube',
    maintainer_email='',

    description='Notebook display helper methods',
    long_description='',

    license='Apache License 2.0',

    tests_require=['pytest'],
    install_requires=[
        'ipywidgets',
        'ipyleaflet',
        'odc_index @ git+https://github.com/opendatacube/dea-proto.git#egg=odc_index&subdirectory=libs/index',
    ],

    packages=['odc.ui'],
    zip_safe=False,
)
