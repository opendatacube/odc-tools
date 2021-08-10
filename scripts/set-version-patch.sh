#!/bin/bash
#
# Edit python file with
#
# __version__ = "MAJOR.MINOR.PATCH"
#
# and set PATCH to supplied value
#
# Example:
#  > set-version-patch _version.py dev33
# If patch is not supplied default to "dev${GITHUB_RUN_NUMBER}"
#

set -o nounset
set -o pipefail
set -o errexit
set -o noclobber

set_version_patch () {
    local version_file="${1:-}"
    local new_patch="${2:-}"

    if [[ -z "${new_patch}" ]]; then
        new_patch="dev${GITHUB_RUN_NUMBER:-0}"
    fi

    # __version__ = 'keep.keep.replace'
    # __version__ = "keep.keep.replace"
    local sed_script="s/\(^ *__version__ *= *[\"'][^'\".]*\.[^'\".]*\)\(\.[^'\".]*\)/\1.${new_patch}/g"

    if [[ -z "${version_file}" || "${version_file}" == "-" ]]; then
        exec sed "${sed_script}"
    else
        echo "Patching ${version_file} with patch: .${new_patch}"
        exec sed "${sed_script}" -i "${version_file}"
    fi
}

set_version_patch "$@"
