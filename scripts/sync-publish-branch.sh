#!/bin/bash

set -o noclobber  # Avoid overlay files (echo "hi" > foo)
set -o errexit    # Used to exit upon error, avoiding cascading errors
set -o pipefail   # Unveils hidden failures
set -o nounset    # Exposes unset variables

B=pypi/publish

_workdir=$(mktemp -d /tmp/odc-tools-XXXXX)
echo "Clone to $_workdir"
git clone --branch $B git@github.com:opendatacube/odc-tools.git $_workdir

echo "Fast forward '$B' to match 'develop' branch"
cd $_workdir
git pull --ff-only origin develop

read -p "Push to GitHub? [y/n]" -n1 ok
echo ""
case $ok in
    y | Y)
        echo "  ok, pushing"
        git push origin pypi/publish
        ;;
    *)
        echo "  ok, won't push to GitHub"
        ;;
esac

read -p "Clean up $_workdir [y/n]" -n1 ok
echo ""
case $ok in
    y | Y)
        echo "  deleting checkout: ${_workdir} in 5 seconds"
        sleep 5
        rm -rfv "${_workdir}"
        ;;
    *)
        echo "  ok, leaving files in: ${_workdir}"
        ;;
esac
