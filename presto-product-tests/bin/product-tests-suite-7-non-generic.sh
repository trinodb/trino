#!/usr/bin/env bash

set -xeuo pipefail

# Suite of product tests which are *not* run dependent on ${HADOOP_BASE_IMAGE} and therefore
# there is no point in running them several times, for different Hadoop distributions.

if test -v HADOOP_BASE_IMAGE; then
    echo "The suite should be run with default HADOOP_BASE_IMAGE. Leave HADOOP_BASE_IMAGE unset." >&2
    exit 1
fi

exit_code=0

# Does not use hadoop
presto-product-tests/bin/run_on_docker.sh \
    singlenode-mysql \
    -g mysql \
    || exit_code=1

# Does not use hadoop
presto-product-tests/bin/run_on_docker.sh \
    singlenode-postgresql \
    -g postgresql \
    || exit_code=1

# Does not use hadoop
presto-product-tests/bin/run_on_docker.sh \
    singlenode-sqlserver \
    -g sqlserver \
    || exit_code=1

presto-product-tests/bin/run_on_docker.sh \
    two-mixed-hives \
    -g two_hives \
    || exit_code=1

presto-product-tests/bin/run_on_docker.sh \
    two-kerberos-hives \
    -g two_hives \
    || exit_code=1

exit "${exit_code}"
