#!/usr/bin/env bash

set -xeuo pipefail

exit_code=0

presto-product-tests/bin/run_on_docker.sh \
    singlenode-mysql \
    -g mysql_connector,mysql \
    || exit_code=1

presto-product-tests/bin/run_on_docker.sh \
    singlenode-postgresql \
    -g postgresql_connector \
    || exit_code=1

presto-product-tests/bin/run_on_docker.sh \
    singlenode-cassandra \
    -g cassandra \
    || exit_code=1

presto-product-tests/bin/run_on_docker.sh \
    singlenode-kerberos-hdfs-impersonation-with-wire-encryption \
    -g storage_formats,cli,hdfs_impersonation,authorization \
    || exit_code=1

presto-product-tests/bin/run_on_docker.sh \
    singlenode-kerberos-kms-hdfs-no-impersonation \
    -g storage_formats \
    || exit_code=1

presto-product-tests/bin/run_on_docker.sh \
    singlenode-kerberos-kms-hdfs-impersonation \
    -g storage_formats \
    || exit_code=1

exit "${exit_code}"
