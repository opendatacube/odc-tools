from setuptools import setup, find_packages

setup(
    name='dea-proto',
    version='0.2',
    license='Apache License 2.0',
    packages=find_packages(),

    author='Open Data Cube',
    author_email='',
    maintainer='Open Data Cube',
    maintainer_email='',

    description='TODO',
    python_requires='>=3.5',
    install_requires=[
        'datacube',
        'click',
        'affine',
        'numpy',
        'rasterio>=1.0.4',
        'odc_aws @ git+https://github.com/opendatacube/dea-proto.git#egg=odc_aws&subdirectory=libs/aws',
        'odc_io @ git+https://github.com/opendatacube/dea-proto.git#egg=odc_io&subdirectory=libs/io',
        'odc_ppt @ git+https://github.com/opendatacube/dea-proto.git#egg=odc_ppt&subdirectory=libs/ppt',
        'odc_apps_cloud @ git+https://github.com/opendatacube/dea-proto.git#egg=odc_apps_cloud&subdirectory=apps/cloud',
    ],
    tests_require=['pytest'],
    entry_points={
        'console_scripts': [
            'dc-index-from-tar = dea.apps.index_from_tar:cli',
        ]
    }
)
