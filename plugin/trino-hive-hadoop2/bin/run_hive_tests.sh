#!/usr/bin/env bash

set -euo pipefail -x

. "${BASH_SOURCE%/*}/common.sh"

if ! check_gib_impacted trino-hive-hadoop2; then
    echo >&2 "Module trino-hive-hadoop2 not present in gib-impacted.log, nothing to do"
    exit 0
fi
cleanup_hadoop_docker_containers
start_hadoop_docker_containers

# obtain Hive version
TESTS_HIVE_VERSION_MAJOR=$(get_hive_major_version)

# generate test data
exec_in_hadoop_master_container sudo -Eu hive beeline -u jdbc:hive2://localhost:10000/default -n hive -f /docker/sql/create-test.sql
exec_in_hadoop_master_container sudo -Eu hive beeline -u jdbc:hive2://localhost:10000/default -n hive -f "/docker/sql/create-test-hive-${TESTS_HIVE_VERSION_MAJOR}.sql"

stop_unnecessary_hadoop_services

HADOOP_MASTER_IP=$(hadoop_master_ip)

# run product tests
pushd "${PROJECT_ROOT}"
set +e
MAVEN_TEST="${MAVEN_TEST:--B --strict-checksums -Dmaven.javadoc.skip=true -Dmaven.source.skip=true -Dair.check.skip-all}"
./mvnw $MAVEN_TEST -pl :trino-hive-hadoop2 test -P test-hive-hadoop2 \
    -DHADOOP_USER_NAME=hive \
    -Dhive.hadoop2.metastoreHost=localhost \
    -Dhive.hadoop2.metastorePort=9083 \
    -Dhive.hadoop2.databaseName=default \
    -Dhive.hadoop2.hiveVersionMajor="${TESTS_HIVE_VERSION_MAJOR}" \
    -Dhive.hadoop2.timeZone=Asia/Kathmandu \
    -Dhive.metastore.thrift.client.socks-proxy="${PROXY}:1180" \
    -Dhive.hdfs.socks-proxy="${PROXY}:1180" \
    -Dhadoop-master-ip="${HADOOP_MASTER_IP}"
EXIT_CODE=$?
set -e
popd

cleanup_hadoop_docker_containers

exit "${EXIT_CODE}"
