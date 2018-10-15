from setuptools import setup, find_packages

setup(
    name='dea-proto',
    version='0.1',
    license='Apache License 2.0',
    packages=find_packages(),
    author='Kirill Kouzoubov',
    author_email='kirill.kouzoubov@ga.gov.au',
    description='TODO',
    python_requires='>=3.5',
    install_requires=['datacube',
                      'click',
                      'boto3',
                      'affine',
                      'numpy',
                      'rasterio',
                      'toolz',
                      ],
    tests_require=['pytest'],
    extras_require=dict(async=[
        'aiohttp',
    ]),
    entry_points={
        'console_scripts': [
            's3-find = dea.apps.s3_find:cli',
            's3-yaml-to-json = dea.apps.s3_to_json_async:cli',
            's3-to-tar = dea.apps.s3_to_tar:cli',
            'dc-index-from-json = dea.apps.index_from_json:cli',
        ]
    }
)
