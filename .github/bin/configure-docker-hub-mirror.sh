#!/bin/bash
set -euo pipefail

readonly MIRROR='https://mirror.gcr.io'
readonly DAEMON_CONFIG='/etc/docker/daemon.json'

current_config=$(mktemp)
updated_config=$(mktemp)
trap 'rm -f "${current_config}" "${updated_config}"' EXIT

# Start from the runner's existing Docker daemon config, if any.
if sudo test -f "${DAEMON_CONFIG}"; then
    sudo cp "${DAEMON_CONFIG}" "${current_config}"
else
    echo '{}' > "${current_config}"
fi

# Preserve existing config and append mirror.gcr.io if it is not already present.
jq --arg mirror "${MIRROR}" '
    if (.["registry-mirrors"] // [] | type) != "array" then
        error("registry-mirrors must be an array")
    else
        (.["registry-mirrors"] // []) as $mirrors |
        .["registry-mirrors"] = if ($mirrors | index($mirror)) then $mirrors else $mirrors + [$mirror] end
    end
' "${current_config}" > "${updated_config}"

# Restart Docker only when the daemon config changed.
if cmp -s "${current_config}" "${updated_config}"; then
    echo "Docker Hub registry mirror is already configured"
else
    sudo install -D -m 0644 "${updated_config}" "${DAEMON_CONFIG}"
    sudo systemctl restart docker
fi

# Show Docker info for CI log verification.
docker info
