#!/usr/bin/env bash

set -euxo pipefail

# Retrieve the script directory.
SCRIPT_DIR="${BASH_SOURCE%/*}"

# Move to the root directory, as that
# is where the Dockerfile resides.
cd ${SCRIPT_DIR}/..

PRESTO_VERSION=$(./mvnw --quiet --batch-mode --non-recursive  exec:exec -Dexec.executable='echo' -Dexec.args='${project.version}')
docker build . --build-arg "PRESTO_VERSION=${PRESTO_VERSION}" -t "presto:${PRESTO_VERSION}"

# Source common testing functions
. ${SCRIPT_DIR}/container-test.sh

test_container "presto:${PRESTO_VERSION}"
