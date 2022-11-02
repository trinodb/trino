#!/bin/bash
set -exuo pipefail

echo "Applying HDP3 hive-site configuration overrides"
apply-site-xml-override /etc/hive/conf/hive-site.xml "/docker/presto-product-tests/conf/environment/singlenode-hdp3/hive-site-overrides.xml"
