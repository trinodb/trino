#!/usr/bin/env bash

set -euo pipefail -x

. "${BASH_SOURCE%/*}/common.sh"

source presto-product-tests/conf/product-tests-config-hdp3.sh

ALLUXIO_DOCKER_COMPOSE_LOCATION="${INTEGRATION_TESTS_ROOT}/conf/alluxio-docker.yml"

function start_alluxio_containers() {
  # stop already running containers
  docker-compose -f "${ALLUXIO_DOCKER_COMPOSE_LOCATION}" down || true

  # catch terminate signals
  trap termination_handler INT TERM

  # pull docker images
  if [[ "${CONTINUOUS_INTEGRATION:-false}" == 'true' ]]; then
    docker-compose -f "${ALLUXIO_DOCKER_COMPOSE_LOCATION}" pull --quiet
  fi

  # start containers
  docker-compose -f "${ALLUXIO_DOCKER_COMPOSE_LOCATION}" up -d

  # start docker logs for hadoop container
  docker-compose -f "${ALLUXIO_DOCKER_COMPOSE_LOCATION}" logs --no-color alluxio-master &

  retry check_alluxio
}

function check_alluxio() {
  run_in_alluxio alluxio fsadmin report
}

function run_in_alluxio() {
    docker exec -e ALLUXIO_JAVA_OPTS=" -Dalluxio.master.hostname=alluxio-master" \
    "$(alluxio_master_container)" $@
}

function alluxio_master_container() {
  docker-compose -f "${ALLUXIO_DOCKER_COMPOSE_LOCATION}" ps -q alluxio-master | grep .
}


function main () {
  cleanup_docker_containers
  start_docker_containers

  start_alluxio_containers

  # generate test data
  exec_in_hadoop_master_container sudo -Eu hdfs hdfs dfs -mkdir /alluxio
  exec_in_hadoop_master_container sudo -Eu hdfs hdfs dfs -chmod 777 /alluxio
  exec_in_hadoop_master_container sudo -Eu hive beeline -u jdbc:hive2://localhost:10000/default -n hive -f /docker/sql/create-test.sql
  exec_in_hadoop_master_container sudo -Eu hive beeline -u jdbc:hive2://localhost:10000/default -n hive -f "/docker/sql/create-test-hive-${TESTS_HIVE_VERSION_MAJOR}.sql"

  stop_unnecessary_hadoop_services

  HADOOP_MASTER_IP=$(hadoop_master_ip)

  run_in_alluxio alluxio table attachdb hive thrift://$(hadoop_master_ip):9083 default || true
  run_in_alluxio alluxio table ls

#    -Dhive.hadoop2.metastoreHost=localhost \
#    -Dhive.hadoop2.metastorePort=9083 \
#    -Dhive.hadoop2.databaseName=default \
#    -Dhive.hadoop2.hiveVersionMajor="${TESTS_HIVE_VERSION_MAJOR}" \
#    -Dhive.hadoop2.timeZone=Asia/Kathmandu \
#    -Dhive.metastore.thrift.client.socks-proxy=${PROXY}:1180 \
#    -Dhive.hdfs.socks-proxy=${PROXY}:1180 \
#    -Dhadoop-master-ip=${HADOOP_MASTER_IP} \

  # run product tests
  pushd ${PROJECT_ROOT}
  set +e
  ./mvnw -B -pl presto-hive-hadoop2 test -P test-hive-hadoop2-alluxio \
    -Dhive.hadoop2.alluxio.host=localhost \
    -Dhive.hadoop2.alluxio.port=19999 \
    -DHADOOP_USER_NAME=hive
  EXIT_CODE=$?
  set -e
  popd

#  cleanup_docker_containers

  exit ${EXIT_CODE}
}

main
