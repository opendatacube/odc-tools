from setuptools import setup

setup(
    name='odc_azure',

    use_scm_version={"root": "../..", "relative_to": __file__},
    setup_requires=['setuptools_scm'],

    author='Open Data Cube',
    author_email='',
    maintainer='Open Data Cube',

    description='Various Azure helper methods (experimental)',
    long_description='',

    license='Apache License 2.0',

    tests_require=['pytest'],
    install_requires=[
        'azure-storage-blob'
    ],

    packages=['odc.azure'],
    zip_safe=False,
)
