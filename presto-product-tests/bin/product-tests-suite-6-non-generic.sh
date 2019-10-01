#!/usr/bin/env bash

set -xeuo pipefail

# Suite of product tests which are *not* run dependent on ${HADOOP_BASE_IMAGE} and therefore
# there is no point in running them several times, for different Hadoop distributions.

if test -v HADOOP_BASE_IMAGE; then
    echo "The suite should be run with default HADOOP_BASE_IMAGE. Leave HADOOP_BASE_IMAGE unset." >&2
    exit 1
fi

exit_code=0

# Uses hadoop, but it irrelevant from these tests' perspective.
presto-product-tests/bin/run_on_docker.sh \
    singlenode-ldap \
    -g ldap \
    -x simba_jdbc \
    || exit_code=1

# We have docker images with KMS on CDH only. TODO (https://github.com/prestosql/presto/issues/1652) create images with HDP and KMS
presto-product-tests/bin/run_on_docker.sh \
    singlenode-kerberos-kms-hdfs-no-impersonation \
    -g storage_formats \
    || exit_code=1

# We have docker images with KMS on CDH only. TODO (https://github.com/prestosql/presto/issues/1652) create images with HDP and KMS
presto-product-tests/bin/run_on_docker.sh \
    singlenode-kerberos-kms-hdfs-impersonation \
    -g storage_formats \
    || exit_code=1

# Does not use hadoop
presto-product-tests/bin/run_on_docker.sh \
    singlenode-cassandra \
    -g cassandra \
    || exit_code=1

# Does not use hadoop
presto-product-tests/bin/run_on_docker.sh \
    singlenode-kafka \
    -g kafka \
    || exit_code=1

exit "${exit_code}"
