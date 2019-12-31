#!/usr/bin/env bash

set -xeuo pipefail

suite_exit_code=0

presto-product-tests/bin/run_on_docker.sh \
    multinode-tls \
    -g smoke,cli,group-by,join,tls \
    || suite_exit_code=1

presto-product-tests/bin/run_on_docker.sh \
    multinode-tls-kerberos \
    -g cli,group-by,join,tls \
    || suite_exit_code=1

presto-product-tests/bin/run_on_docker.sh \
    singlenode-kerberos-hdfs-impersonation-with-wire-encryption \
    -g storage_formats,cli,hdfs_impersonation,authorization \
    || suite_exit_code=1

echo "$0: exiting with ${suite_exit_code}"
exit "${suite_exit_code}"
