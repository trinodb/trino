#!/usr/bin/env bash

set -euo pipefail -x

. "${BASH_SOURCE%/*}/common.sh"

check_vars ADL_NAME ADL_CLIENT_ID ADL_CREDENTIAL ADL_REFRESH_URL

cleanup_hadoop_docker_containers
start_hadoop_docker_containers

test_directory="$(date '+%Y%m%d-%H%M%S')-$(uuidgen | sha1sum | cut -b 1-6)"

# insert Azure credentials
deploy_core_site_xml core-site.xml.adl-template \
    ADL_CLIENT_ID ADL_CREDENTIAL ADL_REFRESH_URL

# restart hive-server2 to apply changes in core-site.xml
exec_in_hadoop_master_container supervisorctl restart hive-server2
retry check_hadoop

create_test_tables "adl://${ADL_NAME}.azuredatalakestore.net/${test_directory}"

stop_unnecessary_hadoop_services

# run product tests
pushd $PROJECT_ROOT
set +e
./mvnw ${MAVEN_TEST:--B} -pl :trino-hive-hadoop2 test -P test-hive-hadoop2-adl \
    -DHADOOP_USER_NAME=hive \
    -Dhive.hadoop2.metastoreHost=localhost \
    -Dhive.hadoop2.metastorePort=9083 \
    -Dhive.hadoop2.databaseName=default \
    -Dhive.hadoop2.adl.name=${ADL_NAME} \
    -Dhive.hadoop2.adl.clientId=${ADL_CLIENT_ID} \
    -Dhive.hadoop2.adl.credential=${ADL_CREDENTIAL} \
    -Dhive.hadoop2.adl.refreshUrl=${ADL_REFRESH_URL} \
    -Dhive.hadoop2.adl.testDirectory=${test_directory}
EXIT_CODE=$?
set -e
popd

cleanup_hadoop_docker_containers

exit ${EXIT_CODE}
