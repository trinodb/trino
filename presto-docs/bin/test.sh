#!/usr/bin/env bash

set -euo pipefail

source "${BASH_SOURCE%/*}/../../presto-product-tests/bin/lib.sh"

function cleanup() {
    #stop_application_runner_containers ${ENVIRONMENT}
    #stop_docker_compose_containers ${ENVIRONMENT}

    # wait for docker containers termination
    wait 2>/dev/null || true
}

function terminate() {
    trap - INT TERM EXIT
    set +e
    cleanup
    exit 130
}

ENVIRONMENT=singlenode

trap terminate INT TERM EXIT

presto_cli="${BASH_SOURCE%/*}/../../presto-cli/target/presto-cli-*-executable.jar"

#environment_compose config
#SERVICES=$(environment_compose config --services | grep -vx 'application-runner\|.*-base')
#environment_compose up --no-color --abort-on-container-exit ${SERVICES} &
#sleep 10
retry check_presto

extract_sql_awk_code=$(cat << EOF
/^[^[:space:]].*/ { sql = 0; }
{ if (sql) { print; }}
/\.\. skip-sql/ { skip = 1; }
/\.\. sql/ { if (skip) { skip = 0; } else { sql = 1; }}
/\.\. code-block:: sql/ { if (skip) { skip = 0; } else { sql = 1; }}
EOF
)

tmp="$(mktemp -d)"
echo "Storing queries in $tmp"
for rst in $(find -name '*rst'); do
    awk "${extract_sql_awk_code}" "${rst}" > "${tmp}/$(basename $rst).sql"
done

for sql in $(find "${tmp}" -name '*sql'); do
    if [ -s "${sql}" ]; then
        echo "Testing queries from ${sql}"
        run_in_application_runner_container /docker/volumes/conf/docker/files/presto-cli.sh --execute "$(cat ${sql})"
    fi
done

trap - INT TERM EXIT
cleanup

exit ${EXIT_CODE}
