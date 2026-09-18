#!/usr/bin/env bash

set -euo pipefail

name="$1"

echo "::error::Requesting merge queue workflow cancellation because ${name} failed"
if ! gh workflow run cancel-merge-queue-workflow.yml \
    --repo "${GITHUB_REPOSITORY}" \
    --ref "${GITHUB_REF_NAME}" \
    -f run_id="${GITHUB_RUN_ID}" \
    -f run_attempt="${GITHUB_RUN_ATTEMPT}"
then
    echo "::warning::Failed to dispatch cancel-merge-queue-workflow.yml"
fi
