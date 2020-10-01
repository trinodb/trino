#!/usr/bin/env bash

set -euxo pipefail

CONTAINER_ID=

function cleanup {
    if [[ ! -z ${CONTAINER_ID:-} ]]; then
        docker stop "${CONTAINER_ID}"
    fi
}

function test_presto_starts {
    local QUERY_PERIOD=5
    local QUERY_RETRIES=30

    trap cleanup EXIT

    local CONTAINER_NAME=$1
    CONTAINER_ID=$(docker run -d --rm "${CONTAINER_NAME}")

    set +e
    I=0
    until RESULT=$(docker exec "${CONTAINER_ID}" presto --execute "SELECT 'success'"); do
        if [[ $((I++)) -ge ${QUERY_RETRIES} ]]; then
            echo "Too many retries waiting for Presto to start."
            break
        fi
        sleep ${QUERY_PERIOD}
    done
    set -e

    # Return proper exit code.
    [[ ${RESULT} == '"success"' ]]
}

function test_javahome {
    local CONTAINER_NAME=$1
    # Check if JAVA_HOME works
    docker run --rm "${CONTAINER_NAME}" /bin/bash -c '$JAVA_HOME/bin/java -version' &> /dev/null

    [[ "$?" == "0" ]]
}

function test_container {
    local CONTAINER_NAME=$1
    test_javahome ${CONTAINER_NAME} && test_presto_starts ${CONTAINER_NAME}
}
