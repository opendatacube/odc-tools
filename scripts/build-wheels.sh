#!/bin/bash

set -eux

WHEEL_DIR=${1:-"$(pwd)/wheels"}
WHEEL_DIR=$(readlink -f ${WHEEL_DIR})
mkdir -p "${WHEEL_DIR}"
# find all folders under apps and libs that have `pyproject.toml` file in them
PP=$(find libs apps -type f -name pyproject.toml -exec dirname '{}' ';')

for p in $PP; do
    echo "Building in ${p}"
    (cd "${p}" && \
         python3 -m build --outdir "${WHEEL_DIR}"
    )
done

echo "Wheels are in: ${WHEEL_DIR}"
