#!/bin/bash

set -euo pipefail

if test $# -gt 0; then
    echo "$0 does not accept arguments" >&2
    exit 32
fi

set -x

tar xf /docker/presto-server.tar.gz -C /docker
exec /docker/presto-server-*/bin/launcher \
  -Dpresto-temporarily-allow-java8=true \
  -Dnode.id="${HOSTNAME}" \
  --etc-dir="/docker/presto-product-tests/conf/presto/etc" \
  --data-dir=/var/presto \
  run
