from setuptools import setup

setup(
    name='odc_aws',

    version='0.1',

    author='Kirill Kouzoubov',
    author_email='kirill.kouzoubov@ga.gov.au',

    description='Various AWS helper methods',
    long_description='',

    license='Apache License 2.0',

    tests_require=['pytest'],
    install_requires=[
        'botocore',
    ],

    packages=['odc.aws'],
    zip_safe=False,
)
