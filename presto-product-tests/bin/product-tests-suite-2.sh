#!/usr/bin/env bash

set -xeuo pipefail

exit_code=0

presto-product-tests/bin/run_on_docker.sh \
    singlenode \
    -g hdfs_no_impersonation,hive_compression \
    -x "${DISTRO_SKIP_GROUP}" \
    || exit_code=1

presto-product-tests/bin/run_on_docker.sh \
    singlenode-kerberos-hdfs-no-impersonation \
    -g hdfs_no_impersonation \
    || exit_code=1

presto-product-tests/bin/run_on_docker.sh \
    singlenode-hdfs-impersonation \
    -g storage_formats,cli,hdfs_impersonation \
    || exit_code=1

presto-product-tests/bin/run_on_docker.sh \
    singlenode-kerberos-hdfs-impersonation \
    -g storage_formats,cli,hdfs_impersonation,authorization,hive_file_header \
    || exit_code=1

exit "${exit_code}"
