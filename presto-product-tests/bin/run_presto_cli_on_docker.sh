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

environment_compose run -p 5008:5008 application-runner /docker/volumes/conf/docker/files/presto-cli.sh "$@"
