#!/bin/bash

set -euo pipefail

if test $# -gt 0; then
    echo "$0 does not accept arguments" >&2
    exit 32
fi

set -x

tar xf /docker/trino-server.tar.gz -C /docker
ln -s "$(echo /docker/trino-server-*/bin/launcher | sed 's@/bin/launcher$@@')" /docker/trino-server

if test -d /docker/presto-init.d; then
    for init_script in /docker/presto-init.d/*; do
        "${init_script}"
    done
fi

if [ -z "$JAVA_HOME" ]; then
    echo "JAVA_HOME not set"
elif [ ! -d "$JAVA_HOME" ]; then
    echo "JAVA_HOME $JAVA_HOME does not exist"
    exit 1
else
    export PATH="${JAVA_HOME}/bin:${PATH}"
fi

exec /docker/trino-server/bin/launcher \
  -Dnode.id="${HOSTNAME}" \
  --etc-dir="/docker/trino-product-tests/conf/trino/etc" \
  --data-dir=/var/trino \
  run
