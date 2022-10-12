#!/usr/bin/env bash

set -euo pipefail

log=$1

safe_grep() {
    grep "$@" || true
}

stats=$(safe_grep ' in io\.trino\.spi\..* has been deprecated$' "$log" | \
    safe_grep -o "$PWD/[^ ]\+.java" | \
    sed "s@$PWD/@@" | \
    sed -E "s@/src/(main|test)/java/.*@:\1@" | sort | uniq -c | sort -nr)

if [ -n "$stats" ]; then
    grep ' in io\.trino\.spi\..* has been deprecated$' "$log" | sed 's/^\[WARNING\]/[ERROR]/'
    echo "[ERROR] Usage of deprecated trino-spi API detected"
    echo "$stats"
    exit 1
fi
