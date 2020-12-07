#!/bin/bash

set -eu

mk_edit_requirements () {
    for d in $(find $(pwd)/libs $(pwd)/apps -name "setup.py" -type f -exec dirname '{}' ';'); do
        echo "-e $d"
    done
}

install_all_in_edit_mode () {
    local reqs=$(mktemp /tmp/requirements-XXXX.txt)
    mk_edit_requirements >> "${reqs}"

    # do double install
    #  1. without deps -- install odc-* but none of their dependents
    #  2. with deps -- now that all `odc-*` are here, reinstall getting all other deps
    python3 -m pip install --no-deps -r "${reqs}"
    python3 -m pip install -r "${reqs}"

    rm "${reqs}"
}

install_all_in_edit_mode
