#!/bin/bash

set -euo pipefail

if test $# -gt 0; then
    echo "$0 does not accept arguments" >&2
    exit 32
fi

set -x

tar xf /docker/presto-server.tar.gz -C /docker
ln -s "$(echo /docker/presto-server-*/bin/launcher | sed 's@/bin/launcher$@@')" /docker/presto-server

if test -d /docker/presto-init.d; then
    for init_script in /docker/presto-init.d/*; do
        "${init_script}"
    done
fi

export JAVA_HOME="/usr/lib/jvm/zulu-11"
export PATH="${JAVA_HOME}/bin:${PATH}"

# When Presto OOM exception occurs, mark Presto as tainted and wait for health check
exec /docker/presto-server/bin/launcher \
  -Dnode.id="${HOSTNAME}" \
  --etc-dir="/docker/presto-product-tests/conf/presto/etc" \
  --data-dir=/var/presto \
  -J "-XX:OnOutOfMemoryError=echo Out of memory occured in pid %p;touch /tmp/presto_oom;sleep 30d" \
  run
