#!/usr/bin/env bash

set -euo pipefail -x

. "${BASH_SOURCE%/*}/common.sh"

check_vars WASB_CONTAINER WASB_ACCOUNT WASB_ACCESS_KEY

cleanup_hadoop_docker_containers
start_hadoop_docker_containers

test_directory="$(date '+%Y%m%d-%H%M%S')-$(uuidgen | sha1sum | cut -b 1-6)"

# insert Azure credentials
deploy_core_site_xml core-site.xml.wasb-template \
    WASB_ACCESS_KEY WASB_ACCOUNT

# restart hive-server2 to apply changes in core-site.xml
exec_in_hadoop_master_container supervisorctl restart hive-server2
retry check_hadoop

create_test_tables "wasb://${WASB_CONTAINER}@${WASB_ACCOUNT}.blob.core.windows.net/${test_directory}"

stop_unnecessary_hadoop_services

# run product tests
pushd $PROJECT_ROOT
set +e
./mvnw ${MAVEN_TEST:--B} -pl :trino-hive-hadoop2 test -P test-hive-hadoop2-wasb \
    -DHADOOP_USER_NAME=hive \
    -Dhive.hadoop2.metastoreHost=localhost \
    -Dhive.hadoop2.metastorePort=9083 \
    -Dhive.hadoop2.databaseName=default \
    -Dhive.hadoop2.wasb.container=${WASB_CONTAINER} \
    -Dhive.hadoop2.wasb.account=${WASB_ACCOUNT} \
    -Dhive.hadoop2.wasb.accessKey=${WASB_ACCESS_KEY} \
    -Dhive.hadoop2.wasb.testDirectory=${test_directory}
EXIT_CODE=$?
set -e
popd

cleanup_hadoop_docker_containers

exit ${EXIT_CODE}
