#!/bin/bash

set -euo pipefail

CONFIG="$1"

shift 1

# creating a copy of the file_auth file for the PrestoCliFileAuthTests.testAuthTokenCacheUpdate()
# to avoid duplicate entries in the original file
if [[ "$CONFIG" == "fileauth" ]]; then
  # check if the same copy exists -> delete in case it exists
    fileAuthCopy=/docker/volumes/conf/presto/etc/file_auth_copy
    if [ -f "$fileAuthCopy" ]; then
      rm -f $fileAuthCopy
    fi
    cp /docker/volumes/conf/presto/etc/file_auth $fileAuthCopy
fi

PRESTO_CONFIG_DIRECTORY="/docker/volumes/conf/presto/etc"
CONFIG_PROPERTIES_LOCATION="${PRESTO_CONFIG_DIRECTORY}/${CONFIG}.properties"

if [[ ! -f "${CONFIG_PROPERTIES_LOCATION}" ]]; then
   echo "${CONFIG_PROPERTIES_LOCATION} does not exist" >&2
   exit 1
fi

/docker/volumes/presto-server/bin/launcher \
  -Dnode.id="${HOSTNAME}" \
  --etc-dir="${PRESTO_CONFIG_DIRECTORY}" \
  --config="${CONFIG_PROPERTIES_LOCATION}" \
  --data-dir=/var/presto \
  "$@"
