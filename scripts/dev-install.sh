#!/bin/bash

set -eu

mk_edit_requirements () {
    for d in $(find $(pwd)/libs $(pwd)/apps -name "setup.py" -type f -exec dirname '{}' ';'); do
        echo "-e $d"
    done
}

install_all_in_edit_mode () {
    local reqs=$(mktemp /tmp/requirements-XXXX.txt)

    # List all libs in -e /path/to/lib mode
    #  this should let pip find all the odc- dependencies locally
    mk_edit_requirements >> "${reqs}"
    python3 -m pip install -r "${reqs}" $@

    rm "${reqs}"
}

install_all_in_edit_mode $@
