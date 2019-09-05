#!/bin/bash

set -eux

# order is important: need to install dependencies first
# leaf nodes: io aws ppt geom
# nodes with odc.* dependencies:
# ------------------------------
#   dtools  <: aws
#   index   <: io
#   dscache <: index
#   ui      <: index
#   aio     <: aws ppt

LIBS="io aws ppt geom"
LIBS+=" dtools index dscache ui"
LIBS+=" aio"

APPS="cloud dc_tools dnsup"

create_pip_tree() {
    for w in $(find . -name "*.whl" | awk -F - '{sub("^./", ""); print $1}')
    do
        local dst="${w//_/-}"
        mkdir -p "${dst}"
        mv "${w}"* "${dst}/"
    done
}

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
         python setup.py bdist_wheel --dist-dir "${WHEEL_DIR}" &&
         python setup.py sdist --dist-dir "${WHEEL_DIR}"
    )
done

(cd ${WHEEL_DIR} && create_pip_tree)
echo "Wheels are in: ${WHEEL_DIR}"
