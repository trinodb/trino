#!/bin/bash

set -euo pipefail

if test $# -gt 0; then
    echo "$0 does not accept arguments" >&2
    exit 32
fi

set -x

tar xf /docker/trino-server.tar.gz -C /docker
ln -s "$(echo /docker/trino-server-*/bin/launcher | sed 's@/bin/launcher$@@')" /docker/trino-server

if test -d /docker/trino-init.d; then
    for init_script in /docker/trino-init.d/*; do
        "${init_script}"
    done
fi

export JAVA_HOME="/usr/lib/jvm/zulu-11"
export PATH="${JAVA_HOME}/bin:${PATH}"

exec /docker/trino-server/bin/launcher \
  -Dnode.id="${HOSTNAME}" \
  --etc-dir="/docker/trino-product-tests/conf/trino/etc" \
  --data-dir=/var/trino \
  run
