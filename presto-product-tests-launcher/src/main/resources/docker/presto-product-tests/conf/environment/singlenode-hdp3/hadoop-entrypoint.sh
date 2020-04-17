#!/bin/bash

set -euo pipefail

fail() {
  echo "$(basename "$0"): $*" >&2
  exit 1
}

DIR="${BASH_SOURCE%/*}"

echo "[$(date)] $0: configuring hadoop services"

apply-site-xml-override /etc/hive/conf/hive-site.xml "${DIR}/hive-site-overrides.xml" || fail "Could not apply hive-site-overrides.xml"

echo "[$(date)] $0: starting hadoop services"
set -x

supervisord -c /etc/supervisord.conf
