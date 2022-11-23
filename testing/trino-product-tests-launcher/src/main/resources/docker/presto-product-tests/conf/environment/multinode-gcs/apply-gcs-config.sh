#!/bin/bash
set -exuo pipefail

echo "Applying GCS core-site configuration overrides"
apply-site-xml-override /etc/hadoop/conf/core-site.xml "/docker/presto-product-tests/conf/environment/multinode-gcs/core-site-overrides.xml"
apply-site-xml-override /etc/hive/conf/hive-site.xml "/docker/presto-product-tests/conf/environment/multinode-gcs/hive-site-overrides.xml"
