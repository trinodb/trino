function cleanup {
    if [[ ! -z ${CONTAINER_ID:-} ]]; then
        docker stop "${CONTAINER_ID}"
    fi
}

function test_trino_starts {
    local QUERY_PERIOD=10
    local QUERY_RETRIES=30

    CONTAINER_ID=
    trap cleanup EXIT

    local CONTAINER_NAME=$1
    local PLATFORM=$2
    CONTAINER_ID=$(docker run -d --rm --platform ${PLATFORM} "${CONTAINER_NAME}")

    set +e
    I=0
    until RESULT=$(docker exec "${CONTAINER_ID}" trino --execute "SELECT 'success'" 2>/dev/null)
    do
        if [[ $((I++)) -ge ${QUERY_RETRIES} ]]; then
            echo "🚨 Too many retries waiting for Trino to start"
            echo "Logs from ${CONTAINER_ID} follow..."
            docker logs "${CONTAINER_ID}"
            break
        fi
        sleep ${QUERY_PERIOD}
    done
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
    docker run --rm --platform ${PLATFORM} "${CONTAINER_NAME}" \
        /bin/bash -c '$JAVA_HOME/bin/java -version' &> /dev/null

    [[ "$?" == "0" ]]
}

function test_container {
    local CONTAINER_NAME=$1
    local PLATFORM=$2
    echo "🐢 Validating ${CONTAINER_NAME} on platform ${PLATFORM}..."
    test_javahome ${CONTAINER_NAME} ${PLATFORM}
    test_trino_starts ${CONTAINER_NAME} ${PLATFORM}
    echo "🎉 Validated ${CONTAINER_NAME} on platform ${PLATFORM}"
}
