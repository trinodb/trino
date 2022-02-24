#!/bin/bash

set -euo pipefail

function get_property() {
    grep "^$1=" "$2" | cut -d'=' -f2
}

scheme=http
port=8080

config=/etc/trino/config.properties
# prefer to use http even if https is enabled
if [ "$(get_property 'http-server.http.enabled' "$config")" == "false" ]; then
    scheme=https
    port=$(get_property http-server.https.port "$config")
else
    port=$(get_property http-server.http.port "$config")
fi

endpoint="$scheme://localhost:$port/v1/info"

# add --insecure to disable certificate verification in curl, in case a self-signed certificate is being used
if ! info=$(curl --fail --silent --show-error --insecure "$endpoint"); then
    echo >&2 "Server is not responding to requests"
    exit 1
fi

if ! grep -q '"starting":\s*false' <<<"$info" >/dev/null; then
    echo >&2 "Server is starting"
    exit 1
fi
