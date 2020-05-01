#!/usr/bin/env bash

set -euo pipefail -x

. "${BASH_SOURCE%/*}/common.sh"

test -v ABFS_CONTAINER
test -v ABFS_ACCOUNT
test -v ABFS_ACCESS_KEY

start_docker_containers

# insert Azure credentials
# TODO replace core-site.xml.abfs-template with apply-site-xml-override.sh
exec_in_hadoop_master_container cp /docker/files/core-site.xml.abfs-template /etc/hadoop/conf/core-site.xml
exec_in_hadoop_master_container sed -i \
    -e "s|%ABFS_ACCESS_KEY%|${ABFS_ACCESS_KEY}|g" \
    -e "s|%ABFS_ACCOUNT%|${ABFS_ACCOUNT}|g" \
    /etc/hadoop/conf/core-site.xml

stop_unnecessary_hadoop_services

HADOOP_MASTER_CONTAINER=$(hadoop_master_container)
# make changes in core-site.xml be effective in hive-metastore
docker exec ${HADOOP_MASTER_CONTAINER} supervisorctl restart hive-metastore
retry check_hadoop

# run product tests
pushd $PROJECT_ROOT
set +e
./mvnw -B -pl presto-hive-hadoop2 test -P test-hive-hadoop2-abfs \
    -DHADOOP_USER_NAME=hive \
    -Dhive.hadoop2.metastoreHost=localhost \
    -Dhive.hadoop2.metastorePort=9083 \
    -Dhive.hadoop2.databaseName=default \
    -Dhive.hadoop2.wasb-container=${ABFS_CONTAINER} \
    -Dhive.hadoop2.wasb-account=${ABFS_ACCOUNT} \
    -Dhive.hadoop2.wasb-access-key=${ABFS_ACCESS_KEY}
EXIT_CODE=$?
set -e
popd

cleanup_docker_containers

exit ${EXIT_CODE}
