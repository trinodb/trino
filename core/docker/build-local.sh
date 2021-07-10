#!/usr/bin/env bash

set -euxo pipefail

SOURCE_DIR="../.."

# Retrieve the script directory.
SCRIPT_DIR="${BASH_SOURCE%/*}"
cd ${SCRIPT_DIR}

# Move to the root directory to run maven for current version.
pushd ${SOURCE_DIR}
TRINO_VERSION=$(./mvnw --quiet help:evaluate -Dexpression=project.version -DforceStdout)
popd

WORK_DIR="$(mktemp -d)"
cp ${SOURCE_DIR}/core/trino-server/target/trino-server-${TRINO_VERSION}.tar.gz ${WORK_DIR}
tar -C ${WORK_DIR} -xzf ${WORK_DIR}/trino-server-${TRINO_VERSION}.tar.gz
rm ${WORK_DIR}/trino-server-${TRINO_VERSION}.tar.gz
cp -R bin ${WORK_DIR}/trino-server-${TRINO_VERSION}
cp -R default ${WORK_DIR}/

cp ${SOURCE_DIR}/client/trino-cli/target/trino-cli-${TRINO_VERSION}-executable.jar ${WORK_DIR}

CONTAINER="trino:${TRINO_VERSION}"

docker build ${WORK_DIR} --pull -f Dockerfile -t ${CONTAINER} --build-arg "TRINO_VERSION=${TRINO_VERSION}"

rm -r ${WORK_DIR}

# Source common testing functions
. container-test.sh

test_container ${CONTAINER}
