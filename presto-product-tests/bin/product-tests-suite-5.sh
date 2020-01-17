#!/usr/bin/env bash

set -xeuo pipefail

suite_exit_code=0

presto-product-tests-launcher/target/presto-product-tests-launcher-*-executable.jar test run \
    --environment singlenode-hive-impersonation \
    -- -g storage_formats,hdfs_impersonation \
    || suite_exit_code=1

presto-product-tests/bin/run_on_docker.sh \
    singlenode-kerberos-hive-impersonation \
    -g storage_formats,hdfs_impersonation,authorization \
    || suite_exit_code=1

echo "$0: exiting with ${suite_exit_code}"
exit "${suite_exit_code}"
