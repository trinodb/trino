#!/bin/bash

set -euo pipefail

if [ -z "${GITHUB_BASE_REF:-}" ] || [ "$GITHUB_BASE_REF" == master ]; then
    echo >&2 "GITHUB_BASE_REF is not set or is master, not fetching it"
    exit 0
fi

git fetch --no-tags --prune origin "$GITHUB_BASE_REF"
