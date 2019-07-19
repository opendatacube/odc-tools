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


for lib in $LIBS; do
    echo "Installing odc-${lib}"
    pip install libs/${lib}
done

for app in $APPS; do
    echo "Installing $app"
    pip install apps/${app}
done
