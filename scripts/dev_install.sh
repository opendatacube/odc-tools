#!/bin/bash

# order is important: need to install dependencies first
# leaf nodes: io aws ppt geom dscache
# nodes with odc.* dependencies:
# ------------------------------
#   dtools <: aws
#   index  <: io
#   ui     <: index
#   aio    <: aws ppt     # skipping aio install, has hard botocore constraints

LIBS="io aws ppt geom dscache"
LIBS+=" dtools index ui"


for lib in $LIBS; do
    echo "Installing odc-${lib}"
    pip install -e libs/${lib} $@
done
