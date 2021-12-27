#!/usr/bin/env bash

if [[ $# -lt 1 ]]; then
    echo "Usage: $0 TRINO_VERSION" >&2
    exit 1
fi

set -euo pipefail

# Retrieve the script directory.
SCRIPT_DIR="${BASH_SOURCE%/*}"
cd ${SCRIPT_DIR}

TRINO_VERSION=$1
SERVER_LOCATION="https://repo1.maven.org/maven2/io/trino/trino-server/${TRINO_VERSION}/trino-server-${TRINO_VERSION}.tar.gz"
CLIENT_LOCATION="https://repo1.maven.org/maven2/io/trino/trino-cli/${TRINO_VERSION}/trino-cli-${TRINO_VERSION}-executable.jar"

WORK_DIR="$(mktemp -d)"
curl -o ${WORK_DIR}/trino-server-${TRINO_VERSION}.tar.gz ${SERVER_LOCATION}
tar -C ${WORK_DIR} -xzf ${WORK_DIR}/trino-server-${TRINO_VERSION}.tar.gz
rm ${WORK_DIR}/trino-server-${TRINO_VERSION}.tar.gz
cp -R bin ${WORK_DIR}/trino-server-${TRINO_VERSION}
cp -R default ${WORK_DIR}/

curl -o ${WORK_DIR}/trino-cli-${TRINO_VERSION}-executable.jar ${CLIENT_LOCATION}
chmod +x ${WORK_DIR}/trino-cli-${TRINO_VERSION}-executable.jar

CONTAINER="trino:${TRINO_VERSION}"

docker build ${WORK_DIR} --pull --platform linux/amd64 -f amd64.dockerfile -t ${CONTAINER}-amd64 --build-arg "TRINO_VERSION=${TRINO_VERSION}"
docker build ${WORK_DIR} --pull --platform linux/arm64 -f arm64.dockerfile -t ${CONTAINER}-arm64 --build-arg "TRINO_VERSION=${TRINO_VERSION}"

rm -r ${WORK_DIR}

# Source common testing functions
. container-test.sh

test_container ${CONTAINER}-amd64 linux/amd64
test_container ${CONTAINER}-arm64 linux/arm64

docker image inspect -f '🚀 Built {{.RepoTags}} {{.Id}}' ${CONTAINER}-amd64
docker image inspect -f '🚀 Built {{.RepoTags}} {{.Id}}' ${CONTAINER}-arm64
