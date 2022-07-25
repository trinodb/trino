#!/usr/bin/env bash

set -euo pipefail -x

. "${BASH_SOURCE%/*}/common.sh"

abort_if_not_gib_impacted

check_vars ABFS_CONTAINER ABFS_ACCOUNT ABFS_ACCESS_KEY

cleanup_hadoop_docker_containers
start_hadoop_docker_containers

test_directory="$(date '+%Y%m%d-%H%M%S')-$(uuidgen | sha1sum | cut -b 1-6)"

# insert Azure credentials
deploy_core_site_xml core-site.xml.abfs-access-key-template \
    ABFS_ACCESS_KEY ABFS_ACCOUNT

# restart hive-server2 to apply changes in core-site.xml
exec_in_hadoop_master_container supervisorctl restart hive-server2
retry check_hadoop

create_test_tables "abfs://${ABFS_CONTAINER}@${ABFS_ACCOUNT}.dfs.core.windows.net/${test_directory}"

stop_unnecessary_hadoop_services

# run product tests
pushd $PROJECT_ROOT
set +e
./mvnw ${MAVEN_TEST:--B} -pl :trino-hive-hadoop2 test -P test-hive-hadoop2-abfs-access-key \
    -DHADOOP_USER_NAME=hive \
    -Dhive.hadoop2.metastoreHost=localhost \
    -Dhive.hadoop2.metastorePort=9083 \
    -Dhive.hadoop2.databaseName=default \
    -Dhive.hadoop2.abfs.container=${ABFS_CONTAINER} \
    -Dhive.hadoop2.abfs.account=${ABFS_ACCOUNT} \
    -Dhive.hadoop2.abfs.accessKey=${ABFS_ACCESS_KEY} \
    -Dhive.hadoop2.abfs.testDirectory="${test_directory}"
EXIT_CODE=$?
set -e
popd

cleanup_hadoop_docker_containers

exit ${EXIT_CODE}
