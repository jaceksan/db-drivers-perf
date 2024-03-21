#!/usr/bin/env bash

mkdir -p db-drivers

DRIVER_FOLDERS="VERTICA"
for i in $DRIVER_FOLDERS; do
  aws s3 sync s3://gdc-tiger-test-data/all_drivers/$i db-drivers/$i
done
