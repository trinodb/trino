#!/bin/bash

set -euo pipefail

echo "[$(date)] $0: configuring hadoop services"

/docker/presto-product-tests/conf/docker/files/apply-site-xml-override.sh /etc/hive/conf/hive-site.xml /docker/presto-product-tests/conf/docker/singlenode-hdp3/hadoop-master/hive-site-overrides.xml

echo "[$(date)] $0: starting hadoop services"
set -x
exec supervisord -c /etc/supervisord.conf
