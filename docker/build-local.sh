#!/usr/bin/env bash

set -euxo pipefail

# Retrieve the script directory.
SCRIPT_DIR="${BASH_SOURCE%/*}"
cd ${SCRIPT_DIR}

# Move to the root directory to run maven for current version.
pushd ..
PRESTO_VERSION=$(./mvnw --quiet help:evaluate -Dexpression=project.version -DforceStdout)
popd

WORK_DIR="$(mktemp -d)"
cp ../presto-server/target/presto-server-${PRESTO_VERSION}.tar.gz ${WORK_DIR}
tar -C ${WORK_DIR} -xzf ${WORK_DIR}/presto-server-${PRESTO_VERSION}.tar.gz
rm ${WORK_DIR}/presto-server-${PRESTO_VERSION}.tar.gz
cp -R bin default ${WORK_DIR}/presto-server-${PRESTO_VERSION}

cp ../presto-cli/target/presto-cli-${PRESTO_VERSION}-executable.jar ${WORK_DIR}

docker build ${WORK_DIR} --pull -f Dockerfile --build-arg "PRESTO_VERSION=${PRESTO_VERSION}" -t "presto:${PRESTO_VERSION}"

rm -r ${WORK_DIR}

# Source common testing functions
. container-test.sh

test_container "presto:${PRESTO_VERSION}"
