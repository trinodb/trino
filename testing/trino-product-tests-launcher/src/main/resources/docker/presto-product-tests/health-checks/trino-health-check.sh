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

# Dump Jacoco report
java -jar /docker/jacoco-cli.jar dump --address localhost --port 19474 --destfile /var/trino/var/jacoco/jacoco-$(date +%s).exec --reset || true
