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

datacube system init --no-default-types --no-init-users
# Created using : datacube metadata list | awk '{print $1}' | xargs datacube metadata show
datacube metadata add "$metadata_catalog"
python -m wget "$product_catalog" -o product_list.csv
tail -n+2 product_list.csv | awk -F, '{print $2}' | xargs datacube -v product add

# Clean up
rm product_list.csv
