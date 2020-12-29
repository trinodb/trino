#!/usr/bin/env bash

set -euxo pipefail

# Retrieve the script directory.
SCRIPT_DIR="${BASH_SOURCE%/*}"
cd ${SCRIPT_DIR}

if [[ $# -lt 1 ]]; then
    echo "Usage: $0 PRESTO_VERSION"
    echo "Missing PRESTO_VERSION"
    exit 1
fi

PRESTO_VERSION=$1
PRESTO_LOCATION="https://repo1.maven.org/maven2/io/trino/trino-server/${TRINO_VERSION}/trino-server-${TRINO_VERSION}.tar.gz"
CLIENT_LOCATION="https://repo1.maven.org/maven2/io/prestosql/presto-cli/${TRINO_VERSION}/trino-cli-${TRINO_VERSION}-executable.jar"

WORK_DIR="$(mktemp -d)"
curl -o ${WORK_DIR}/trino-server-${TRINO_VERSION}.tar.gz ${TRINO_LOCATION}
tar -C ${WORK_DIR} -xzf ${WORK_DIR}/trino-server-${TRINO_VERSION}.tar.gz
rm ${WORK_DIR}/trino-server-${TRINO_VERSION}.tar.gz
cp -R bin default ${WORK_DIR}/trino-server-${TRINO_VERSION}

curl -o ${WORK_DIR}/trino-cli-${TRINO_VERSION}-executable.jar ${CLIENT_LOCATION}
chmod +x ${WORK_DIR}/trino-cli-${TRINO_VERSION}-executable.jar

docker build ${WORK_DIR} --pull -f Dockerfile -t "presto:${TRINO_VERSION}" --build-arg "TRINO_VERSION=${TRINO_VERSION}"

rm -r ${WORK_DIR}

# Source common testing functions
. container-test.sh

test_container "presto:${TRINO_VERSION}"
