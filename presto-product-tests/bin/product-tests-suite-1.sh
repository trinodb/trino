#!/usr/bin/env bash

set -xeuo pipefail

suite_exit_code=0

presto-product-tests-launcher/bin/run-launcher test run \
    --environment multinode \
    -- -x quarantine,big_query,storage_formats,profile_specific_tests,tpcds,cassandra,mysql,postgresql,kafka,hive_compression,"${DISTRO_SKIP_GROUP}" \
    || suite_exit_code=1

echo "$0: exiting with ${suite_exit_code}"
exit "${suite_exit_code}"
