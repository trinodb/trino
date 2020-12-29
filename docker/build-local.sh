#!/usr/bin/env bash

set -euxo pipefail

# Retrieve the script directory.
SCRIPT_DIR="${BASH_SOURCE%/*}"
cd ${SCRIPT_DIR}

# Move to the root directory to run maven for current version.
pushd ..
TRINO_VERSION=$(./mvnw --quiet help:evaluate -Dexpression=project.version -DforceStdout)
popd

WORK_DIR="$(mktemp -d)"
cp ../trino-server/target/trino-server-${TRINO_VERSION}.tar.gz ${WORK_DIR}
tar -C ${WORK_DIR} -xzf ${WORK_DIR}/trino-server-${TRINO_VERSION}.tar.gz
rm ${WORK_DIR}/trino-server-${TRINO_VERSION}.tar.gz
cp -R bin default ${WORK_DIR}/trino-server-${TRINO_VERSION}

cp ../trino-cli/target/trino-cli-${TRINO_VERSION}-executable.jar ${WORK_DIR}

docker build ${WORK_DIR} --pull -f Dockerfile --build-arg "TRINO_VERSION=${TRINO_VERSION}" -t "presto:${TRINO_VERSION}"

rm -r ${WORK_DIR}

# Source common testing functions
. container-test.sh

test_container "presto:${TRINO_VERSION}"
