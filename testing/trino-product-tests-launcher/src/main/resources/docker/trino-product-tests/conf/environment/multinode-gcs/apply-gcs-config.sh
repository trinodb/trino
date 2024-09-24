#!/bin/bash
set -exuo pipefail

echo "Applying GCS core-site configuration overrides"
apply-site-xml-override /etc/hadoop/conf/core-site.xml "/docker/trino-product-tests/conf/environment/multinode-gcs/core-site-overrides.xml"
apply-site-xml-override /etc/hive/conf/hive-site.xml "/docker/trino-product-tests/conf/environment/multinode-gcs/hive-site-overrides.xml"
