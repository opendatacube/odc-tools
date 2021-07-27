#!/bin/bash
############################################################
# This script allows creation of all metadata/products     #
# in an ODC instance from a given CSV catalog definition   #
# In local development activate <odc> conda environment    #
# In kubernetes pod executor run from <datacube-index>     #
# container                                                #
############################################################
set -o errexit
set -o pipefail
set -o nounset

product_catalog=$1
metadata_catalog=$2

echo "BOOTSTRAP: Initialising DB."
datacube system init --no-default-types --no-init-users
# Created using : datacube metadata list | awk '{print $1}' | xargs datacube metadata show
echo "BOOTSTRAP: Adding metadata types."
datacube metadata add "$metadata_catalog"
echo "BOOTSTRAP: Adding products."
dc-sync-products "$product_catalog"
