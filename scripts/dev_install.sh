#!/bin/bash

# order is important: need to install dependencies first
# leaf nodes: io aws ppt geom algo
# nodes with odc.* dependencies:
# ------------------------------
#   dtools  <: aws
#   index   <: io
#   dscache <: index
#   ui      <: index
#   aio     <: aws ppt

LIBS="io aws ppt geom algo"
LIBS+=" dtools index dscache ui"
LIBS+=" aio"

# if aio enabled install aiobotocore
if [ -z "${LIBS##*aio*}" ] ; then
    pip install -U 'aiobotocore[boto3]'
fi

for lib in $LIBS; do
    echo "Installing odc-${lib}"
    pip install -e libs/${lib} $@
done
