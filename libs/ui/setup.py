from setuptools import setup

setup(
    name='odc_ui',

    version='1',
    author='Open Data Cube',
    author_email='',
    maintainer='Open Data Cube',
    maintainer_email='',

    description='Notebook display helper methods',
    long_description='',

    license='Apache License 2.0',

    tests_require=['pytest'],
    install_requires=['ipywidgets'],

    packages=['odc.ui'],
    zip_safe=False,
)
