#!/usr/bin/env bash
set -euo pipefail

if test $# -gt 0; then
    echo "$0 does not accept arguments" >&2
    exit 32
fi

set -x

trap exit INT

echo "Running services with supervisord"

supervisord -c /etc/supervisord.conf
