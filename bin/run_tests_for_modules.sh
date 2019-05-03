#!/bin/bash

set -euo pipefail

cd "${BASH_SOURCE%/*}/.."

for module in $(echo "$@" | tr , ' '); do
    "${module}/bin/test.sh"
done
