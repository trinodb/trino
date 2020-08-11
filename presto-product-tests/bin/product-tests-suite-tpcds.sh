#!/usr/bin/env bash

set -xeuo pipefail

DISTRO_SKIP_GROUP="${DISTRO_SKIP_GROUP:-}"
DISTRO_SKIP_TEST="${DISTRO_SKIP_TEST:-}"

suite_exit_code=0

# TODO: Results for q72 need to be fixed. https://github.com/prestosql/presto/issues/4564
presto-product-tests-launcher/bin/run-launcher test run \
    --environment multinode \
    -- -g tpcds -x "${DISTRO_SKIP_GROUP}" -e sql_tests.testcases.tpcds.q72,"${DISTRO_SKIP_TEST}" \
    || suite_exit_code=1


echo "$0: exiting with ${suite_exit_code}"
exit "${suite_exit_code}"
