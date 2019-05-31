#!/usr/bin/env bash

set -euxo pipefail

CONTAINER_ID=

function cleanup {
    if [[ ! -z ${CONTAINER_ID:-} ]]; then
        docker stop "${CONTAINER_ID}"
    fi
}

function test_container {
    local QUERY_TIMEOUT=150
    local QUERY_PERIOD=15
    local QUERY_RETRIES=$((QUERY_TIMEOUT/QUERY_PERIOD))

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
