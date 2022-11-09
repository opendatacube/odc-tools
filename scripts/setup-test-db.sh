#!/bin/bash

set -eu

function start_db() {
    {
        pg_ctl -D ${pgdata} -l "${pgdata}/pg.log" start
    } || {
        # a common reason for failure is that there is already something running on port 5432
        # if that's the case, kill that process and retry
        # if not or it still fails, print the log
        if [[ "$(<${pgdata}/pg.log)" =~ .*"Address already in use".* ]]; then
            sudo kill -9 $(sudo lsof -i:5432 | awk 'NR==2 {print $2}')
            pg_ctl -D ${pgdata} -l "${pgdata}/pg.log" start || cat "${pgdata}/pg.log"
        else   
            cat "${pgdata}/pg.log"
            exit 1
        fi
    }
}

if [ -d "$(pwd)/.dbdata" ]; then
    rm -rf "$(pwd)/.dbdata"
fi

export DATACUBE_DB_URL=postgresql:///datacube
pgdata=$(pwd)/.dbdata
initdb -D ${pgdata} --auth-host=md5 --encoding=UTF8
start_db
createdb datacube
datacube system init
# add any new metadata types
datacube metadata add "https://raw.githubusercontent.com/GeoscienceAustralia/dea-config/master/product_metadata/eo3_sentinel_ard.odc-type.yaml"
