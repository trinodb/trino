#!/usr/bin/env bash

set -xeuo pipefail

suite_exit_code=0

presto-product-tests/bin/run_on_docker.sh \
    multinode \
    -x quarantine,big_query,storage_formats,profile_specific_tests,tpcds,cassandra,mysql,postgresql,kafka,simba_jdbc,hive_compression,"${DISTRO_SKIP_GROUP}" \
    || suite_exit_code=1

echo "$0: exiting with ${suite_exit_code}"
exit "${suite_exit_code}"
