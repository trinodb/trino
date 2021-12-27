#!/usr/bin/env bash

function cleanup {
    if [[ -n ${CONTAINER_ID:-} ]]; then
        docker rm -f "${CONTAINER_ID}"
    fi
}

function test_trino_starts {
    local QUERY_PERIOD=10
    local QUERY_RETRIES=90

    CONTAINER_ID=
    trap cleanup EXIT

    local CONTAINER_NAME=$1
    local CONTAINER_CLI_NAME=$2
    local PLATFORM=$3
    # We aren't passing --rm here to make sure container is available for inspection in case of failures
    CONTAINER_ID=$(docker run -d --platform "${PLATFORM}" "${CONTAINER_NAME}")
    SERVER_IP=$(docker inspect --format '{{range.NetworkSettings.Networks}}{{.IPAddress}}{{end}}' "$CONTAINER_ID")

    set +e
    I=0
    until docker inspect "${CONTAINER_ID}" --format "{{json .State.Health.Status }}" | grep -q '"healthy"'; do
        if [[ $((I++)) -ge ${QUERY_RETRIES} ]]; then
            echo "🚨 Too many retries waiting for Trino to start"
            echo "Logs from ${CONTAINER_ID} follow..."
            docker logs "${CONTAINER_ID}"
            break
        fi
        sleep ${QUERY_PERIOD}
    done
    # test both the cli in the server image and the cli image
    if ! RESULT=$(docker exec "${CONTAINER_ID}" trino --execute "SELECT 'success'" 2>/dev/null) ||
        ! RESULT=$(docker run --rm --platform "${PLATFORM}" "${CONTAINER_CLI_NAME}" --server "$SERVER_IP:8080" --execute "SELECT 'success'" 2>/dev/null); then
        echo "🚨 Failed to execute a query after Trino container started"
    fi
    set -e

    cleanup
    trap - EXIT

    # Return proper exit code.
    [[ ${RESULT} == '"success"' ]]
}

function test_javahome {
    local CONTAINER_NAME=$1
    local PLATFORM=$2
    # Check if JAVA_HOME works
    docker run --rm --platform "${PLATFORM}" "${CONTAINER_NAME}" \
        /bin/bash -c '$JAVA_HOME/bin/java -version' &> /dev/null

    [[ $? == "0" ]]
}

function test_container {
    local CONTAINER_NAME=$1
    local CONTAINER_CLI_NAME=$2
    local PLATFORM=$3
    echo "🐢 Validating ${CONTAINER_NAME} and ${CONTAINER_CLI_NAME} on platform ${PLATFORM}..."
    test_javahome "${CONTAINER_NAME}" "${PLATFORM}"
    test_trino_starts "${CONTAINER_NAME}" "${CONTAINER_CLI_NAME}" "${PLATFORM}"
    echo "🎉 Validated ${CONTAINER_NAME} and ${CONTAINER_CLI_NAME} on platform ${PLATFORM}"
}
