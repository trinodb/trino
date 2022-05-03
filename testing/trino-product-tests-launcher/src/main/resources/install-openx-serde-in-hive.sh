#!/bin/bash
set -xeuo pipefail
# This works for HDP3
cp --no-clobber --verbose -t /usr/hdp/*/hive/lib /docker/json-serde-jar-with-dependencies.jar || \
# This works for CDP7.1
cp --no-clobber --verbose -t /var/lib/hive/lib /docker/json-serde-jar-with-dependencies.jar || \
# This works for CDH6 and CDH5
cp --no-clobber --verbose -t /usr/lib/hive/lib /docker/json-serde-jar-with-dependencies.jar
