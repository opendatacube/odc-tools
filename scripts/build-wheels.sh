#!/bin/bash

set -eux

WHEEL_DIR=${1:-"$(pwd)/wheels"}
WHEEL_DIR=$(readlink -f ${WHEEL_DIR})
mkdir -p "${WHEEL_DIR}"
# find all folders under apps and libs that have `setup.py` file in them
PP=$(find libs apps -type f -name setup.py -exec dirname '{}' ';')

for p in $PP; do
    echo "Building in ${p}"
    (cd "${p}" && \
         python setup.py bdist_wheel --dist-dir "${WHEEL_DIR}" && \
         python setup.py sdist --dist-dir "${WHEEL_DIR}"
    )
done

echo "Wheels are in: ${WHEEL_DIR}"
