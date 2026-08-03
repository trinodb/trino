#!/bin/bash
set -euo pipefail

resolve_ipv4() {
    local host="$1"
    getent ahostsv4 "$host" | awk 'NR == 1 { print $1; exit }'
}

fe_alias="${DORIS_FE_ALIAS:-doris-fe}"
fe_ip="$(resolve_ipv4 "$fe_alias")"

if [[ -z "${fe_ip}" ]]; then
    echo "Could not resolve IPv4 address for FE alias '${fe_alias}'" >&2
    exit 1
fi

export FE_SERVERS="fe1:${fe_ip}:${FE_EDIT_LOG_PORT:-9010}"

exec bash init_fe.sh
