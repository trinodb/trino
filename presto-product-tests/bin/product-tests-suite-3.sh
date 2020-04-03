#!/usr/bin/env bash

set -xeuo pipefail

suite_exit_code=0

presto-product-tests-launcher/bin/run-launcher test run \
    --environment multinode-tls \
    -- -g smoke,cli,group-by,join,tls \
    || suite_exit_code=1

presto-product-tests-launcher/bin/run-launcher test run \
    --environment multinode-tls-kerberos \
    -- -g cli,group-by,join,tls \
    || suite_exit_code=1

presto-product-tests-launcher/bin/run-launcher test run \
    --environment singlenode-kerberos-hdfs-impersonation-with-wire-encryption \
    -- -g storage_formats,cli,hdfs_impersonation,authorization -x iceberg \
    || suite_exit_code=1

echo "$0: exiting with ${suite_exit_code}"
exit "${suite_exit_code}"
