#!/usr/bin/env bash

set -xeuo pipefail

DISTRO_SKIP_GROUP="${DISTRO_SKIP_GROUP:-}"
DISTRO_SKIP_TEST="${DISTRO_SKIP_TEST:-}"

suite_exit_code=0

presto-product-tests-launcher/bin/run-launcher test run \
    --environment multinode-tls \
    -- -g smoke,cli,group-by,join,tls -x "${DISTRO_SKIP_GROUP}" -e "${DISTRO_SKIP_TEST}" \
    || suite_exit_code=1

presto-product-tests-launcher/bin/run-launcher test run \
    --environment multinode-tls-kerberos \
    -- -g cli,group-by,join,tls -x "${DISTRO_SKIP_GROUP}" -e "${DISTRO_SKIP_TEST}" \
    || suite_exit_code=1

presto-product-tests-launcher/bin/run-launcher test run \
    --environment singlenode-kerberos-hdfs-impersonation-with-wire-encryption \
    -- -g storage_formats,cli,hdfs_impersonation,authorization -x iceberg,"${DISTRO_SKIP_GROUP}" -e "${DISTRO_SKIP_TEST}" \
    || suite_exit_code=1

# Smoke run af a few tests in environment with dfs.data.transfer.protection=true. Arbitrary tests which access HDFS data were chosen.
presto-product-tests-launcher/bin/run-launcher test run \
    --environment singlenode-kerberos-hdfs-impersonation-with-data-protection \
    -- -t TestHiveStorageFormats.testOrcTableCreatedInPresto,TestHiveCreateTable.testCreateTable  -x "${DISTRO_SKIP_GROUP}" -e "${DISTRO_SKIP_TEST}" \
    || suite_exit_code=1

echo "$0: exiting with ${suite_exit_code}"
exit "${suite_exit_code}"
