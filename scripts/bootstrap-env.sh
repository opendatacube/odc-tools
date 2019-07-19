#!/bin/bash

# ppa:nextgis/ppa
# to build needs:
#    apt install libgdal-dev libudunits2-0
# to run:
#    apt install gdal-data libgdal20 libudunits2-0

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

set -eux

exit_with_error () {
    echo "$1"
    exit 1
}

env_dir=${1:-"./test_env"}

[ -d "${env_dir}" ] || python3 -m venv "${env_dir}"
[ -f "${env_dir}/bin/activate" ] || exit_with_error "Not a valid python environment: ${env_dir}"

source "${env_dir}/bin/activate"
echo "Using pip: $(which pip)"
pip install --upgrade pip
pip install wheel
pip install setuptools-scm
pip install 'aiobotocore[boto3]'
pip install GDAL=="$(gdal-config --version)"
pip install datacube

"${SCRIPT_DIR}/travis.sh"
