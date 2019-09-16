#!/usr/bin/env bash

set -xeuo pipefail

presto-product-tests/bin/run_on_docker.sh \
    singlenode \
    -g hdfs_no_impersonation

presto-product-tests/bin/run_on_docker.sh \
    singlenode-kerberos-hdfs-no-impersonation \
    -g hdfs_no_impersonation

presto-product-tests/bin/run_on_docker.sh \
    singlenode-hdfs-impersonation \
    -g storage_formats,cli,hdfs_impersonation

presto-product-tests/bin/run_on_docker.sh \
    singlenode-kerberos-hdfs-impersonation \
    -g storage_formats,cli,hdfs_impersonation,authorization,hive_file_header

presto-product-tests/bin/run_on_docker.sh \
    singlenode-kerberos-hdfs-impersonation-cross-realm \
    -g storage_formats,cli,hdfs_impersonation

