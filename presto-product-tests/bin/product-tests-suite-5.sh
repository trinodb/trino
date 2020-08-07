#!/usr/bin/env bash

set -xeuo pipefail

DISTRO_SKIP_GROUP="${DISTRO_SKIP_GROUP:-}"
DISTRO_SKIP_TEST="${DISTRO_SKIP_TEST:-}"

suite_exit_code=0

presto-product-tests-launcher/bin/run-product-tests --suite suite-5 --environment singlenode-hive-impersonation \
    -g storage_formats,hdfs_impersonation -x "${DISTRO_SKIP_GROUP}" -e "${DISTRO_SKIP_TEST}" \
    || suite_exit_code=1

presto-product-tests-launcher/bin/run-product-tests --suite suite-5 --environment singlenode-kerberos-hive-impersonation \
    -g storage_formats,hdfs_impersonation,authorization -x "${DISTRO_SKIP_GROUP}" -e "${DISTRO_SKIP_TEST}" \
    || suite_exit_code=1

presto-product-tests-launcher/bin/run-product-tests --suite suite-5 --environment multinode-hive-caching \
    -g hive_caching,storage_formats -x iceberg,"${DISTRO_SKIP_GROUP}" -e "${DISTRO_SKIP_TEST}" \
    || suite_exit_code=1

echo "$0: exiting with ${suite_exit_code}"
exit "${suite_exit_code}"
