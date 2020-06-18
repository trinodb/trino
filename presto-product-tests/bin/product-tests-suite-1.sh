#!/usr/bin/env bash

set -xeuo pipefail

DISTRO_SKIP_GROUP="${DISTRO_SKIP_GROUP:-}"
DISTRO_SKIP_TEST="${DISTRO_SKIP_TEST:-}"

suite_exit_code=0

presto-product-tests-launcher/bin/run-launcher test run \
    --environment multinode \
    -- \
    -x big_query,storage_formats,profile_specific_tests,tpcds,hive_compression,"${DISTRO_SKIP_GROUP}" \
    -e "${DISTRO_SKIP_TEST}" \
    || suite_exit_code=1

echo "$0: exiting with ${suite_exit_code}"
exit "${suite_exit_code}"
