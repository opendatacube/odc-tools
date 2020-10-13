#!/bin/bash

exec odc-stats run-gm tasks.db \
  --verbose \
  --public \
  --threads 40 \
  --location s3://deafrica-stats-processing/kk/gm-1210/ \
  $@
