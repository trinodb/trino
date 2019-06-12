#!/usr/bin/env bash

set -xeuo pipefail

presto-product-tests/bin/run_on_docker.sh \
    singlenode-ldap \
    -g ldap \
    -x simba_jdbc

presto-product-tests/bin/run_on_docker.sh \
    multinode-tls \
    -g smoke,cli,group-by,join,tls

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
    singlenode-kafka \
    -g kafka
