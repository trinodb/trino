#!/usr/bin/env bash

set -euo pipefail -x

. "${BASH_SOURCE%/*}/common.sh"

abort_if_not_gib_impacted

export ALLUXIO_BASE_IMAGE="alluxio/alluxio"
export ALLUXIO_IMAGE_TAG="2.1.2"

ALLUXIO_DOCKER_COMPOSE_LOCATION="${INTEGRATION_TESTS_ROOT}/conf/alluxio-docker.yml"

function check_alluxio() {
    run_in_alluxio alluxio fsadmin report
}

function run_in_alluxio() {
    docker exec -e ALLUXIO_JAVA_OPTS=" -Dalluxio.master.hostname=localhost" \
           "$(alluxio_master_container)" $@
}

# Arguments:
#   $1: container name
function get_alluxio_container() {
    docker-compose -f "${ALLUXIO_DOCKER_COMPOSE_LOCATION}" ps -q "$1" | grep .
}

function alluxio_master_container() {
    get_alluxio_container alluxio-master
}

function main () {
    cleanup_docker_containers "${DOCKER_COMPOSE_LOCATION}" "${ALLUXIO_DOCKER_COMPOSE_LOCATION}"
    start_docker_containers "${DOCKER_COMPOSE_LOCATION}" "${ALLUXIO_DOCKER_COMPOSE_LOCATION}"
    retry check_hadoop
    retry check_alluxio & # data can be generated while we wait for alluxio to start

    # obtain Hive version
    TESTS_HIVE_VERSION_MAJOR=$(get_hive_major_version)

    # generate test data
    exec_in_hadoop_master_container sudo -Eu hdfs hdfs dfs -mkdir /alluxio
    exec_in_hadoop_master_container sudo -Eu hdfs hdfs dfs -chmod 777 /alluxio
    exec_in_hadoop_master_container sudo -Eu hive beeline -u jdbc:hive2://localhost:10000/default -n hive -f /docker/sql/create-test.sql
    exec_in_hadoop_master_container sudo -Eu hive beeline -u jdbc:hive2://localhost:10000/default -n hive -f "/docker/sql/create-test-hive-${TESTS_HIVE_VERSION_MAJOR}.sql"

    # Alluxio currently doesn't support views
    exec_in_hadoop_master_container sudo -Eu hive beeline -u jdbc:hive2://localhost:10000/default -n hive -e 'DROP VIEW trino_test_view;'

    stop_unnecessary_hadoop_services

    wait # make sure alluxio has started

    run_in_alluxio alluxio table attachdb hive thrift://hadoop-master:9083 default
    run_in_alluxio alluxio table ls default

    # run product tests
    pushd ${PROJECT_ROOT}
    set +e
    ./mvnw ${MAVEN_TEST:--B} -pl :trino-hive-hadoop2 test -P test-hive-hadoop2-alluxio \
           -Dhive.hadoop2.alluxio.host=localhost \
           -Dhive.hadoop2.alluxio.port=19998 \
           -Dhive.hadoop2.hiveVersionMajor="${TESTS_HIVE_VERSION_MAJOR}" \
           -Dhive.hadoop2.timeZone=Asia/Kathmandu \
           -DHADOOP_USER_NAME=hive
    EXIT_CODE=$?
    set -e
    popd

    cleanup_docker_containers "${DOCKER_COMPOSE_LOCATION}" "${ALLUXIO_DOCKER_COMPOSE_LOCATION}"

    exit ${EXIT_CODE}
}

main
