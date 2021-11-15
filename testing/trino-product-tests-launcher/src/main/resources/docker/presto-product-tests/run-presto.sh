#!/bin/bash

set -euo pipefail

if test $# -gt 0; then
    echo "$0 does not accept arguments" >&2
    exit 32
fi

set -x

tar xf /docker/presto-server.tar.gz -C /docker
ln -s "$(echo /docker/trino-server-*/bin/launcher | sed 's@/bin/launcher$@@')" /docker/presto-server

if test -d /docker/presto-init.d; then
    for init_script in /docker/presto-init.d/*; do
        "${init_script}"
    done
fi

if [ -z "$JAVA_HOME" ]; then
    echo "JAVA_HOME must be set"
    exit 1
fi

if [ ! -d "$JAVA_HOME" ]; then
    echo "JAVA_HOME $JAVA_HOME does not exist"
    exit 1
fi

export PATH="${JAVA_HOME}/bin:${PATH}"

exec /docker/presto-server/bin/launcher \
  -Dnode.id="${HOSTNAME}" \
  --etc-dir="/docker/presto-product-tests/conf/presto/etc" \
  --data-dir=/var/trino \
  run
