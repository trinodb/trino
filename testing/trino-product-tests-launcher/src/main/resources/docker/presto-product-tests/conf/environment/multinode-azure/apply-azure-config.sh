#!/bin/bash
set -exuo pipefail

echo "Applying HDP3 core-site configuration overrides"
apply-site-xml-override /etc/hadoop/conf/core-site.xml "/docker/presto-product-tests/conf/environment/multinode-azure/core-site-overrides.xml"
