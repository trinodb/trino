#!/usr/bin/env bash

set -xeuo pipefail

presto-product-tests/bin/run_on_docker.sh \
    multinode-tls \
    -g smoke,cli,group-by,join,tls

presto-product-tests/bin/run_on_docker.sh \
    multinode-tls-kerberos \
    -g cli,group-by,join,tls

presto-product-tests/bin/run_on_docker.sh \
    singlenode-ldap \
    -g ldap \
    -x simba_jdbc

presto-product-tests/bin/run_on_docker.sh \
    singlenode-sqlserver \
    -g sqlserver

presto-product-tests/bin/run_on_docker.sh \
    singlenode-kafka \
    -g kafka
