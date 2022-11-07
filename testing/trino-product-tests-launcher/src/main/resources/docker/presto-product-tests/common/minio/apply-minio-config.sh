#!/bin/bash

set -exuo pipefail

fail() {
  echo "$(basename "$0"): $*" >&2
  exit 1
}

echo "Applying configuration overrides from minio-core-site-overrides.xml"

apply-site-xml-override /etc/hadoop/conf/core-site.xml "/docker/presto-product-tests/common/minio/minio-core-site-overrides.xml" || fail "Could not apply minio-core-site-overrides.xml"
