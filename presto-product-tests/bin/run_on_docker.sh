#!/bin/bash

set -euo pipefail

source "${BASH_SOURCE%/*}/lib.sh"

function run_product_tests() {
  local REPORT_DIR="${PRODUCT_TESTS_ROOT}/target/test-reports"
  rm -rf "${REPORT_DIR}"
  mkdir -p "${REPORT_DIR}"
  run_in_application_runner_container /docker/volumes/conf/docker/files/run-tempto.sh "$@" &
  PRODUCT_TESTS_PROCESS_ID=$!
  wait ${PRODUCT_TESTS_PROCESS_ID}
  local PRODUCT_TESTS_EXIT_CODE=$?

  #make the files in $REPORT_DIR modifiable by everyone, as they were created by root (by docker)
  run_in_application_runner_container chmod -R 777 "/docker/volumes/test-reports"

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

function usage() {
  echo "Usage: run_on_docker.sh <`getAvailableEnvironments | tr '\n' '|' | sed 's/|$//'`> <product test args>"
  exit 1
 }

if [[ $# == 0 ]]; then
  usage
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

if [[ ${CONTINUOUS_INTEGRATION:-false} = true ]]; then
    environment_compose pull --quiet
fi

# catch terminate signals
trap terminate INT TERM EXIT

# display how test environment is configured
environment_compose config
SERVICES=$(environment_compose config --services | grep -vx 'application-runner\|.*-base')
environment_compose up --no-color --abort-on-container-exit ${SERVICES} &

# wait until hadoop processes are started
retry check_hadoop

# wait until presto is started
retry check_presto

# run product tests
set +e
run_product_tests "$@"
EXIT_CODE=$?
set -e

# execution finished successfully
# disable trap, run cleanup manually
trap - INT TERM EXIT
cleanup

exit ${EXIT_CODE}
