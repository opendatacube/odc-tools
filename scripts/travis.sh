#!/bin/bash

set -eux

# order is important: need to install dependencies first
# leaf nodes: io aws ppt geom dscache
# nodes with odc.* dependencies:
# ------------------------------
#   dtools <: aws
#   index  <: io
#   ui     <: index
#   aio    <: aws ppt

LIBS="io aws ppt geom dscache"
LIBS+=" dtools index ui"
LIBS+=" aio"

APPS="cloud dc_tools dnsup"
PP=()
WHEEL_DIR=$(pwd)/wheels
mkdir -p "${WHEEL_DIR}"

for lib in $LIBS; do
    echo "Installing odc-${lib}"
    (cd libs/${lib} && python setup.py install)
    PP+=(libs/${lib})
done

for app in $APPS; do
    echo "Installing $app"
    (cd apps/${app} && python setup.py install)
    PP+=(apps/${app})
done

for p in "${PP[@]}"; do
    (cd "${p}" &&
     python setup.py bdist_wheel --dist-dir "${WHEEL_DIR}")
done

echo "Wheels are in: ${WHEEL_DIR}"
