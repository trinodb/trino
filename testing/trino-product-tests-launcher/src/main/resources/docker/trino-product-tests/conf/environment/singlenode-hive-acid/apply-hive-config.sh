#!/bin/bash
set -exuo pipefail

echo "Applying HDP3 hive-site configuration overrides"
apply-site-xml-override /etc/hive/conf/hive-site.xml "/docker/trino-product-tests/conf/environment/singlenode-hive-acid/hive-site-overrides.xml"
