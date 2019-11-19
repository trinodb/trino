#!/bin/bash

set -euo pipefail

source "${BASH_SOURCE%/*}/lib.sh"

function retry() {
  local END
  local EXIT_CODE

  END=$(($(date +%s) + 600))

  while (( $(date +%s) < $END )); do
    set +e
    "$@"
    EXIT_CODE=$?
    set -e

    if [[ ${EXIT_CODE} == 0 ]]; then
      break
    fi
    sleep 5
  done

  return ${EXIT_CODE}
}

function hadoop_master_container(){
  environment_compose ps -q hadoop-master | grep .
}

function check_hadoop() {
  HADOOP_MASTER_CONTAINER=$(hadoop_master_container)
  docker exec ${HADOOP_MASTER_CONTAINER} supervisorctl status hive-server2 | grep -iq running && \
    docker exec ${HADOOP_MASTER_CONTAINER} netstat -lpn | grep -iq 0.0.0.0:10000
}

function check_any_container_is_up() {
  environment_compose ps -q | grep .
}

function run_in_application_runner_container() {
  local CONTAINER_NAME
  CONTAINER_NAME=$(environment_compose run -p 5007:5007 -d application-runner "$@")
  echo "Showing logs from $CONTAINER_NAME:"
  docker logs -f $CONTAINER_NAME
  return $(docker inspect --format '{{.State.ExitCode}}' $CONTAINER_NAME)
}

function check_presto() {
  run_in_application_runner_container /docker/presto-product-tests/conf/docker/files/presto-cli.sh --execute "SHOW CATALOGS" | grep -iq hive
}

function run_product_tests() {
  local REPORT_DIR="${PRODUCT_TESTS_ROOT}/target/test-reports"
  rm -rf "${REPORT_DIR}"
  mkdir -p "${REPORT_DIR}"
  run_in_application_runner_container /docker/presto-product-tests/conf/docker/files/run-tempto.sh "$@" &
  PRODUCT_TESTS_PROCESS_ID=$!
  wait ${PRODUCT_TESTS_PROCESS_ID}
  local PRODUCT_TESTS_EXIT_CODE=$?

  #make the files in $REPORT_DIR modifiable by everyone, as they were created by root (by docker)
  run_in_application_runner_container chmod -R 777 "/docker/test-reports"

  return ${PRODUCT_TESTS_EXIT_CODE}
}

function cleanup() {
  stop_application_runner_containers ${ENVIRONMENT}

  if [[ "${LEAVE_CONTAINERS_ALIVE_ON_EXIT:-false}" != "true" ]]; then
    stop_docker_compose_containers ${ENVIRONMENT}
  fi

  # wait for docker containers termination
  wait 2>/dev/null || true
}

function terminate() {
  trap - INT TERM EXIT
  set +e
  cleanup
  exit 130
}

if [[ $# == 0 ]]; then
  usage "<product test arguments>"
fi

ENVIRONMENT=$1
shift 1

# Get the list of valid environments
if [[ ! -f "$DOCKER_CONF_LOCATION/$ENVIRONMENT/compose.sh" ]]; then
  usage
fi

# check docker and docker compose installation
docker-compose version
docker version

stop_all_containers
remove_empty_property_files

if [[ ${CONTINUOUS_INTEGRATION:-false} = true ]]; then
    environment_compose pull --quiet
fi

# catch terminate signals
trap terminate INT TERM EXIT

# display how test environment is configured
environment_compose config
SERVICES=$(environment_compose config --services | grep -vx 'application-runner\|.*-base')
environment_compose up --no-color --abort-on-container-exit ${SERVICES} &

# Wait for `environment_compose up` to create docker network *without* creating any new containers.
# Otherwise docker-compose may created network twice and subsequently fail.
retry check_any_container_is_up

# wait until hadoop processes are started
retry check_hadoop

# wait until presto is started
retry check_presto

# run product tests
set +e -x
run_product_tests "$@"
EXIT_CODE=$?
set -e
echo "Product tests exited with ${EXIT_CODE}"

# execution finished successfully
# disable trap, run cleanup manually
trap - INT TERM EXIT
cleanup

exit ${EXIT_CODE}
