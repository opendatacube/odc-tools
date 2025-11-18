#!/bin/bash
# Perform a development (or editable) install of all the libraries and apps contained
# within the odc-tools repository.

set -eu

mk_edit_requirements () {
    for d in $(find $(pwd)/libs $(pwd)/apps -name "pyproject.toml" -type f -exec dirname '{}' ';'); do
        echo "-e $d -r $d/pyproject.toml"
    done
}

install_all_in_edit_mode () {
    uv venv --allow-existing
    # List all libs in -e /path/to/lib mode
    #  this should let pip find all the odc- dependencies locally
    uv pip install $(mk_edit_requirements | grep -v "/dist/") $@
}

install_all_in_edit_mode $@
