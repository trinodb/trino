#!/usr/bin/env bash
set -euo pipefail

if test $# -gt 0; then
    echo "$0 does not accept arguments" >&2
    exit 32
fi

# Check if TrinoServer is listed as running Java process
PID=$(jps | grep TrinoServer | cut -f1 -d ' ')

# Dump process information for debugging purposes
jstack -l $PID > /var/trino/var/log/stack.log || true
