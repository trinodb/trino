#!/bin/bash
set -euo pipefail

resolve_ipv4() {
    local host="$1"
    getent ahostsv4 "$host" | awk 'NR == 1 { print $1; exit }'
}

fe_alias="${DORIS_FE_ALIAS:-doris-fe}"
fe_ip="$(resolve_ipv4 "$fe_alias")"
be_ip="$(hostname -i | awk '{print $1}')"

if [[ -z "${fe_ip}" ]]; then
    echo "Could not resolve IPv4 address for FE alias '${fe_alias}'" >&2
    exit 1
fi

if [[ -z "${be_ip}" ]]; then
    echo "Could not determine IPv4 address for BE container" >&2
    exit 1
fi

export FE_SERVERS="fe1:${fe_ip}:${FE_EDIT_LOG_PORT:-9010}"
export BE_ADDR="${be_ip}:${BE_HEARTBEAT_PORT:-9050}"

exec bash entry_point.sh
