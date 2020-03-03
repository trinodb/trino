#!/usr/bin/env bash

set -euo pipefail -x

. ${BASH_SOURCE%/*}/common.sh

test -v AZURE_HDINSIGHT_METASTORE_HOST
test -v AZURE_HIVE_WASB_ACCOUNT
test -v AZURE_HIVE_WASB_ACCESS_KEY

cd ${PROJECT_ROOT}

./mvnw -B -pl presto-hive-hadoop2 test -P test-hive-hadoop2-hdinsight \
  -Dhive.hadoop2.timeZone=UTC \
  -DHADOOP_USER_NAME=hive \
  -Dhive.hadoop2.metastoreHost="${AZURE_HDINSIGHT_METASTORE_HOST}" \
  -Dhive.hadoop2.metastorePort=9083 \
  -Dhive.hadoop2.databaseName=hdinsight_test \
  -Dhive.hadoop2.wasb-account="${AZURE_HIVE_WASB_ACCOUNT}" \
  -Dhive.hadoop2.wasb-access-key="${AZURE_HIVE_WASB_ACCESS_KEY}"
