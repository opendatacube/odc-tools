from setuptools import setup, find_packages

setup(
    name='dea-proto',
    version='0.2',
    license='Apache License 2.0',
    packages=find_packages(),
    author='Kirill Kouzoubov',
    author_email='kirill.kouzoubov@ga.gov.au',
    description='TODO',
    python_requires='>=3.5',
    install_requires=['datacube',
                      'click',
                      'affine',
                      'numpy',
                      'rasterio>=1.0.4',
                      'toolz',
                      'zstandard',
                      'lmdb',
                      'aiobotocore',
                      'botocore',
                      'odc_aws',
                      ],
    extras_require={
        'GCP': ['google-cloud-storage'],
        'THREDDS': ['thredds_crawler', 'requests']
    },
    tests_require=['pytest'],
    entry_points={
        'console_scripts': [
            'thredds-to-tar = dea.apps.thredds_to_tar:cli [THREDDS]',
            'gs-to-tar = dea.apps.gs_to_tar:cli [GCP]',
            's3-find = dea.apps.s3_find:cli',
            's3-inventory-dump = dea.apps.s3_inventory:cli',
            's3-to-tar = dea.apps.s3_to_tar:cli',
            'dc-index-from-tar = dea.apps.index_from_tar:cli',
            'slurpy = dea.apps.slurpy:cli',
            'dstiler = dea.apps.dstiler:cli',
        ]
    },
    dependency_links=[
        'git+https://github.com/opendatacube/dea-proto.git#egg=odc_aws&subdirectory=libs/aws',
    ]
)
