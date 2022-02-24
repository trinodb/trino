#!/usr/bin/env bash

GITHUB_REF_NAME="$(git rev-parse --abbrev-ref HEAD)"
ARTIFACT_SERVER_PATH="/tmp/act-artifacts"

cat <<EOF > /tmp/act-pull-request.json
{
  "pull_request": {
    "head": {
      "ref": "${GITHUB_REF_NAME}"
    },
    "base": {
      "ref": "${GITHUB_BASE_REF:-master}"
    }
  }
}
EOF

mkdir -p "$ARTIFACT_SERVER_PATH"
act --artifact-server-path "$ARTIFACT_SERVER_PATH" --eventpath /tmp/act-pull-request.json --env GITHUB_REF_NAME "$@"
