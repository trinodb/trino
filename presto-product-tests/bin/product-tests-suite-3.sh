#!/usr/bin/env bash

set -xeuo pipefail

presto-product-tests/bin/run_on_docker.sh \
    singlenode-mysql \
    -g mysql_connector,mysql

presto-product-tests/bin/run_on_docker.sh \
    singlenode-postgresql \
    -g postgresql_connector

presto-product-tests/bin/run_on_docker.sh \
    singlenode-cassandra \
    -g cassandra

presto-product-tests/bin/run_on_docker.sh \
    singlenode-kerberos-hdfs-impersonation-with-wire-encryption \
    -g storage_formats,cli,hdfs_impersonation,authorization

presto-product-tests/bin/run_on_docker.sh \
    singlenode-kerberos-kms-hdfs-no-impersonation \
    -g storage_formats

# TODO enable avro when adding Metastore impersonation
presto-product-tests/bin/run_on_docker.sh \
    singlenode-kerberos-kms-hdfs-impersonation \
    -g storage_formats \
    -x avro
