#!/usr/bin/env bash

set -euxo pipefail

# Retrieve the script directory.
SCRIPT_DIR="${BASH_SOURCE%/*}"

# Move to the root directory, as that
# is where the Dockerfile resides.
cd ${SCRIPT_DIR}/..

if [[ $# -lt 1 ]]; then
    echo "Usage: $0 PRESTO_VERSION"
    echo "Missing PRESTO_VERSION"
    exit 1
fi

PRESTO_VERSION=$1
PRESTO_LOCATION="https://repo1.maven.org/maven2/io/prestosql/presto-server/${PRESTO_VERSION}/presto-server-${PRESTO_VERSION}.tar.gz"
CLIENT_LOCATION="https://repo1.maven.org/maven2/io/prestosql/presto-cli/${PRESTO_VERSION}/presto-cli-${PRESTO_VERSION}-executable.jar"
docker build . -t "presto:${PRESTO_VERSION}" --build-arg "PRESTO_VERSION=${PRESTO_VERSION}" --build-arg "CLIENT_LOCATION=${CLIENT_LOCATION}" --build-arg "PRESTO_LOCATION=${PRESTO_LOCATION}"

# Source common testing functions
. ${SCRIPT_DIR}/container-test.sh

test_container "presto:${PRESTO_VERSION}"
