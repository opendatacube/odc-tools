#!/usr/bin/env python
import sys

from setuptools import setup, find_packages

try:
    from setuptools_rust import RustExtension
except ImportError:
    import subprocess

    errno = subprocess.call([sys.executable, "-m", "pip", "install", "setuptools-rust"])
    if errno:
        print("Please install setuptools-rust package")
        raise SystemExit(errno)
    else:
        from setuptools_rust import RustExtension

setup_requires = ["setuptools-rust>=0.10.1", "wheel", "setuptools_scm"]

setup(
    name="odc_algo",
    use_scm_version={"root": "../..", "relative_to": __file__},
    setup_requires=setup_requires,
    author="Open Data Cube",
    author_email="",
    maintainer="Open Data Cube",
    description="Miscellaneous Algorithmic helper methods",
    long_description="",
    license="Apache License 2.0",
    tests_require=["pytest"],
    install_requires=[
        "affine",
        "numexpr",
        "dask",
        "distributed",
        "xarray",
        "numpy",
        "toolz",
        "odc-index",
        "datacube",
        "scikit-image",
        "dask_image",
    ],
    extras_require={'hdstats': ["hdstats>=0.1.7.post5"]},
    packages=["odc.algo"] + find_packages(),
    zip_safe=False,
    rust_extensions=[RustExtension("odc.algo.backend")],
)
