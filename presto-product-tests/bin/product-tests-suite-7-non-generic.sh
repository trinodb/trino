#!/usr/bin/env bash

set -xeuo pipefail

# Suite of product tests which are *not* run dependent on ${HADOOP_BASE_IMAGE} and therefore
# there is no point in running them several times, for different Hadoop distributions.

if test -v HADOOP_BASE_IMAGE; then
    echo "The suite should be run with default HADOOP_BASE_IMAGE. Leave HADOOP_BASE_IMAGE unset." >&2
    exit 1
fi

suite_exit_code=0

# Does not use hadoop
presto-product-tests-launcher/bin/run-launcher test run \
    --environment singlenode-mysql \
    --reports-dir="${BASH_SOURCE%/*}"/../../product-tests-reports/suite-7-non-generic/singlenode-mysql \
    -- -g mysql \
    || suite_exit_code=1

# Does not use hadoop
presto-product-tests-launcher/bin/run-launcher test run \
    --environment singlenode-postgresql \
    --reports-dir="${BASH_SOURCE%/*}"/../../product-tests-reports/suite-7-non-generic/singlenode-postgresql \
    -- -g postgresql \
    || suite_exit_code=1

# Does not use hadoop
presto-product-tests-launcher/bin/run-launcher test run \
    --environment singlenode-sqlserver \
    --reports-dir="${BASH_SOURCE%/*}"/../../product-tests-reports/suite-7-non-generic/singlenode-sqlserver \
    -- -g sqlserver \
    || suite_exit_code=1

presto-product-tests-launcher/bin/run-launcher test run \
    --environment singlenode-spark-iceberg \
    --reports-dir="${BASH_SOURCE%/*}"/../../product-tests-reports/suite-7-non-generic/singlenode-spark-iceberg \
    -- -g iceberg -x storage_formats \
    || suite_exit_code=1

# Environment not set up on CDH. (TODO run on HDP 2.6 and HDP 3.1)
presto-product-tests-launcher/bin/run-launcher test run \
    --environment singlenode-kerberos-hdfs-impersonation-cross-realm \
    --reports-dir="${BASH_SOURCE%/*}"/../../product-tests-reports/suite-7-non-generic/singlenode-kerberos-hdfs-impersonation-cross-realm \
    -- -g storage_formats,cli,hdfs_impersonation \
    || suite_exit_code=1

presto-product-tests-launcher/bin/run-launcher test run \
    --environment two-mixed-hives \
    --reports-dir="${BASH_SOURCE%/*}"/../../product-tests-reports/suite-7-non-generic/two-mixed-hives \
    -- -g two_hives \
    || suite_exit_code=1

presto-product-tests-launcher/bin/run-launcher test run \
    --environment two-kerberos-hives \
    --reports-dir="${BASH_SOURCE%/*}"/../../product-tests-reports/suite-7-non-generic/two-kerberos-hives \
    -- -g two_hives \
    || suite_exit_code=1

presto-product-tests-launcher/bin/run-launcher test run \
    --environment singlenode-ldap-bind-dn \
    --reports-dir="${BASH_SOURCE%/*}"/../../product-tests-reports/suite-7-non-generic/singlenode-ldap-bind-dn \
    -- -g ldap \
    || suite_exit_code=1

echo "$0: exiting with ${suite_exit_code}"
exit "${suite_exit_code}"
