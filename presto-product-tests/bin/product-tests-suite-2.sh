#!/usr/bin/env bash

set -xeuo pipefail

suite_exit_code=0

presto-product-tests/bin/run_on_docker.sh \
    singlenode \
    -g hdfs_no_impersonation,hive_compression \
    -x "${DISTRO_SKIP_GROUP}" \
    || suite_exit_code=1

presto-product-tests/bin/run_on_docker.sh \
    singlenode-kerberos-hdfs-no-impersonation \
    -g hdfs_no_impersonation \
    || suite_exit_code=1

presto-product-tests/bin/run_on_docker.sh \
    singlenode-hdfs-impersonation \
    -g storage_formats,cli,hdfs_impersonation \
    || suite_exit_code=1

presto-product-tests/bin/run_on_docker.sh \
    singlenode-kerberos-hdfs-impersonation \
    -g storage_formats,cli,hdfs_impersonation,authorization,hive_file_header \
    || suite_exit_code=1

echo "$0: exiting with ${suite_exit_code}"
exit "${suite_exit_code}"
