#!/usr/bin/env bash

set -xeuo pipefail

exit_code=0

presto-product-tests/bin/run_on_docker.sh \
    multinode-tls \
    -g smoke,cli,group-by,join,tls \
    || exit_code=1

presto-product-tests/bin/run_on_docker.sh \
    multinode-tls-kerberos \
    -g cli,group-by,join,tls \
    || exit_code=1

presto-product-tests/bin/run_on_docker.sh \
    singlenode-ldap \
    -g ldap \
    -x simba_jdbc \
    || exit_code=1

presto-product-tests/bin/run_on_docker.sh \
    singlenode-sqlserver \
    -g sqlserver \
    || exit_code=1

presto-product-tests/bin/run_on_docker.sh \
    singlenode-kafka \
    -g kafka \
    || exit_code=1

exit "${exit_code}"
