from setuptools import setup

setup(
    name='dea_dnsup',

    version='0.0.1',
    author='Open Data Cube',
    author_email='',
    maintainer='Open Data Cube',
    maintainer_email='',

    description='Update DNS records for EC2 instances using route53 service',
    long_description='',

    license='Apache License 2.0',

    tests_require=['pytest'],
    install_requires=[
        'odc_aws @ git+https://github.com/opendatacube/dea-proto.git#egg=odc_aws&subdirectory=libs/aws',
    ],

    entry_points={
        'console_scripts': [
            'dea-dnsup = dea_dnsup:cli',
        ]
    },

    packages=['dea_dnsup'],
    zip_safe=False,
)
