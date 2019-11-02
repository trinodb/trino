#!/bin/bash

set -euo pipefail

source "${BASH_SOURCE%/*}/lib.sh"

if [[ $# == 0 ]]; then
  usage "<presto cli arguments>"
fi

ENVIRONMENT=$1
shift 1

# Get the list of valid environments
if [[ ! -f "$DOCKER_CONF_LOCATION/$ENVIRONMENT/compose.sh" ]]; then
  usage
fi

# create CLI history file (otherwise Docker creates it as a directory)
export PRESTO_CLI_HISTORY_FILE="/tmp/presto_history_docker"
touch "${PRESTO_CLI_HISTORY_FILE}"

environment_compose run -p 5008:5008 application-runner /docker/presto-product-tests/conf/docker/files/presto-cli.sh "$@"
