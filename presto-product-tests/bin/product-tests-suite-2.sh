#!/usr/bin/env bash

set -xeuo pipefail

DISTRO_SKIP_GROUP="${DISTRO_SKIP_GROUP:-}"
DISTRO_SKIP_TEST="${DISTRO_SKIP_TEST:-}"

suite_exit_code=0

presto-product-tests-launcher/bin/run-launcher test run \
    --environment singlenode \
    -- -g hdfs_no_impersonation,hive_compression -x "${DISTRO_SKIP_GROUP}" -e "${DISTRO_SKIP_TEST}" \
    || suite_exit_code=1

presto-product-tests-launcher/bin/run-launcher test run \
    --environment singlenode-kerberos-hdfs-no-impersonation \
    -- -g storage_formats,hdfs_no_impersonation -x iceberg,"${DISTRO_SKIP_GROUP}" -e "${DISTRO_SKIP_TEST}" \
    || suite_exit_code=1

presto-product-tests-launcher/bin/run-launcher test run \
    --environment singlenode-hdfs-impersonation \
    -- -g storage_formats,cli,hdfs_impersonation -x "${DISTRO_SKIP_GROUP}" -e "${DISTRO_SKIP_TEST}" \
    || suite_exit_code=1

presto-product-tests-launcher/bin/run-launcher test run \
    --environment singlenode-kerberos-hdfs-impersonation \
    -- -g storage_formats,cli,hdfs_impersonation,authorization,hive_file_header -x iceberg,"${DISTRO_SKIP_GROUP}" -e "${DISTRO_SKIP_TEST}" \
    || suite_exit_code=1

echo "$0: exiting with ${suite_exit_code}"
exit "${suite_exit_code}"
