#!/bin/bash
set -exuo pipefail

echo "Applying hive-site configuration overrides for Spark"
apply-site-xml-override /etc/hive/conf/hive-site.xml "/docker/trino-product-tests/conf/environment/singlenode-spark-iceberg/hive-site-overrides.xml"
