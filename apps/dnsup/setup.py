from setuptools import setup

setup(
    name='dea_dnsup',

    use_scm_version={"root": "../..", "relative_to": __file__},
    setup_requires=['setuptools_scm'],

    author='Open Data Cube',
    author_email='',
    maintainer='Open Data Cube',
    maintainer_email='',

    description='Update DNS records for EC2 instances using route53 service',
    long_description='',

    license='Apache License 2.0',

    tests_require=['pytest'],
    install_requires=[
        'odc_aws',
    ],

    entry_points={
        'console_scripts': [
            'dea-dnsup = dea_dnsup:cli',
        ]
    },

    packages=['dea_dnsup'],
    zip_safe=False,
)
