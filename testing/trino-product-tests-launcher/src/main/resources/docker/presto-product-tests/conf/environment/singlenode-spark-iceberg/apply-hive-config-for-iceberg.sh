#!/bin/bash

set -exuo pipefail

fail() {
  echo "$(basename "$0"): $*" >&2
  exit 1
}

echo "Applying hive-site configuration overrides for Spark"

wget https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-hive-runtime/0.12.1/iceberg-hive-runtime-0.12.1.jar
cp iceberg-hive-runtime-0.12.1.jar /usr/hdp/current/hive-client/auxlib
apply-site-xml-override /etc/hive/conf/hive-site.xml "/docker/presto-product-tests/conf/environment/singlenode-spark-iceberg/hive-site-overrides.xml" || fail "Could not apply hive-site-overrides.xml"
