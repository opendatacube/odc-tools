from setuptools import setup

setup(
    name='odc_thredds',

    use_scm_version={"root": "../..", "relative_to": __file__},
    setup_requires=['setuptools_scm'],

    author='Open Data Cube',
    author_email='',
    maintainer='Open Data Cube',

    description='Various Thredds helper methods (for NCI)',
    long_description='',

    license='Apache License 2.0',

    tests_require=['pytest'],
    install_requires=[
        'thredds_crawler',
        'requests',
        'aiohttp'
    ],

    packages=['odc.thredds'],
    zip_safe=False,
)
