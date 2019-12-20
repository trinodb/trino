#!/bin/bash

set -euo pipefail

CONFIG="$1"

shift 1

PRESTO_CONFIG_DIRECTORY="/docker/presto-product-tests/conf/presto/etc"
CONFIG_PROPERTIES_LOCATION="${PRESTO_CONFIG_DIRECTORY}/${CONFIG}.properties"

if [[ ! -f "${CONFIG_PROPERTIES_LOCATION}" ]]; then
   echo "${CONFIG_PROPERTIES_LOCATION} does not exist" >&2
   exit 1
fi

set -x
tar xf /docker/presto-server.tar.gz -C /docker
/docker/presto-server-*/bin/launcher \
  -Dnode.id="${HOSTNAME}" \
  --etc-dir="${PRESTO_CONFIG_DIRECTORY}" \
  --config="${CONFIG_PROPERTIES_LOCATION}" \
  --data-dir=/var/presto \
  "$@"
