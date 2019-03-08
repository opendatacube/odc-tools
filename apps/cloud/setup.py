from setuptools import setup

setup(
    name='odc_apps_cloud',

    version='1',
    author='Open Data Cube',
    author_email='',
    maintainer='Open Data Cube',
    maintainer_email='',

    description='CLI utils for working with objects/files the cloud',
    long_description='',

    license='Apache License 2.0',

    tests_require=['pytest'],
    install_requires=[
        'odc_aws @ git+https://github.com/opendatacube/dea-proto.git#egg=odc_aws&subdirectory=libs/aws',
        'odc_io @ git+https://github.com/opendatacube/dea-proto.git#egg=odc_io&subdirectory=libs/io',
        'odc_aio @ git+https://github.com/opendatacube/dea-proto.git#egg=odc_aio&subdirectory=libs/aio',
        'odc_ppt @ git+https://github.com/opendatacube/dea-proto.git#egg=odc_ppt&subdirectory=libs/ppt',
        "click",
    ],

    extras_require={
        'GCP': ['google-cloud-storage'],
        'THREDDS': ['thredds_crawler', 'requests']
    },

    entry_points={
        'console_scripts': [
            'thredds-to-tar = odc.apps.cloud.thredds_to_tar:cli [THREDDS]',
            'gs-to-tar = odc.apps.cloud.gs_to_tar:cli [GCP]',
            's3-find = odc.apps.cloud.s3_find:cli',
            's3-inventory-dump = odc.apps.cloud.s3_inventory:cli',
            's3-to-tar = odc.apps.cloud.s3_to_tar:cli',
        ]
    },

    packages=['odc.apps.cloud'],
    zip_safe=False,
)
