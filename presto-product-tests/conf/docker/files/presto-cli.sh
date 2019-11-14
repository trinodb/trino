#!/bin/bash

set -euxo pipefail

java \
    `#-agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=5008` \
    -jar /docker/presto-cli-executable.jar \
    ${CLI_ARGUMENTS} \
    "$@"
