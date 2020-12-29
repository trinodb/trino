#!/bin/bash
set -eo pipefail

# https://github.com/docker-library/healthcheck/blob/master/cassandra/docker-healthcheck
host="$(hostname --ip-address || echo '127.0.0.1')"

if cqlsh -u cassandra -p cassandra "$host" < /dev/null; then
	exit 0
fi

exit 1
