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
PRESTO_LOCATION="https://repo1.maven.org/maven2/io/prestosql/presto-server/${PRESTO_VERSION}/presto-server-${PRESTO_VERSION}.tar.gz"
CLIENT_LOCATION="https://repo1.maven.org/maven2/io/prestosql/presto-cli/${PRESTO_VERSION}/presto-cli-${PRESTO_VERSION}-executable.jar"

WORK_DIR="$(mktemp -d)"
curl -o ${WORK_DIR}/presto-server-${PRESTO_VERSION}.tar.gz ${PRESTO_LOCATION}
tar -C ${WORK_DIR} -xzf ${WORK_DIR}/presto-server-${PRESTO_VERSION}.tar.gz
rm ${WORK_DIR}/presto-server-${PRESTO_VERSION}.tar.gz
cp -R bin default ${WORK_DIR}/presto-server-${PRESTO_VERSION}

curl -o ${WORK_DIR}/presto-cli-${PRESTO_VERSION}-executable.jar ${CLIENT_LOCATION}
chmod +x ${WORK_DIR}/presto-cli-${PRESTO_VERSION}-executable.jar

docker build ${WORK_DIR} --pull -f Dockerfile -t "presto:${PRESTO_VERSION}" --build-arg "PRESTO_VERSION=${PRESTO_VERSION}"

rm -r ${WORK_DIR}

# Source common testing functions
. container-test.sh

test_container "presto:${PRESTO_VERSION}"
