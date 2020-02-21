#!/bin/bash

set -euo pipefail

function join_by {
    local IFS="$1"
    shift
    echo "$*"
}

set -x

tempto_config_files=()
tempto_config_files+=(tempto-configuration.yaml) # this comes from classpath
tempto_config_files+=(/docker/presto-product-tests/conf/tempto/tempto-configuration-for-docker-default.yaml)
tempto_config_files+=(/docker/presto-product-tests/conf/tempto/tempto-configuration-profile-config-file.yaml)
tempto_config_files+=("${TEMPTO_ENVIRONMENT_CONFIG_FILE:-/dev/null}")

exec java \
 `# TODO implement sth like --debug switch in tests-launcher` \
 `#-agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=5007` \
 -Djava.util.logging.config.file=/docker/presto-product-tests/conf/tempto/logging.properties \
 -Duser.timezone=Asia/Kathmandu \
 `#TODO "-cp", "/docker/presto-jdbc.jar:..." "io.prestosql.tests.TemptoProductTestRunner"` \
 -jar /docker/test.jar \
 `#TODO --report-dir /docker/test-reports/$(date "+%Y-%m-%dT%H:%M:%S")` \
 --config "$(join_by , "${tempto_config_files[@]}")" \
 "$@"
