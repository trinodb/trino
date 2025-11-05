#!/usr/bin/env bash

set -exuo pipefail

# Enable spooling protocol
echo "protocol.spooling.enabled=true" >> ${CONTAINER_TRINO_CONFIG_PROPERTIES}
echo "protocol.spooling.shared-secret-key=jxTKysfCBuMZtFqUf8UJDQ1w9ez8rynEJsJqgJf66u0=" >> ${CONTAINER_TRINO_CONFIG_PROPERTIES}
# Enable direct storage access so we can test fetching against the storage
echo "protocol.spooling.retrieval-mode=storage" >> ${CONTAINER_TRINO_CONFIG_PROPERTIES}
