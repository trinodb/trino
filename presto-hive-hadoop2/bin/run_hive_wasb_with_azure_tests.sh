#!/usr/bin/env bash

set -euo pipefail -x

. "${BASH_SOURCE%/*}"/common.sh

test -v AZURE_WASB_TABLE_NAME
test -v WASB_CONTAINER
test -v WASB_ACCOUNT
test -v WASB_ACCESS_KEY

pushd $PROJECT_ROOT

./mvnw -B -pl presto-hive-hadoop2 test -P test-hive-hadoop2-wasb \
  -DHADOOP_USER_NAME=hive \
  -Dhive.hadoop2.metastoreHost="${AZURE_HDINSIGHT_METASTORE_HOST}" \
  -Dhive.hadoop2.metastorePort=9083 \
  -Dhive.hadoop2.databaseName=default \
  -Dhive.hadoop2.tableName="${AZURE_WASB_TABLE_NAME}" \
  -Dhive.hadoop2.wasb-container="${WASB_CONTAINER}" \
  -Dhive.hadoop2.wasb-account="${WASB_ACCOUNT}" \
  -Dhive.hadoop2.wasb-access-key="${WASB_ACCESS_KEY}"
