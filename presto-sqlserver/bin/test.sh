#!/bin/bash

set -exuo pipefail

cd "${BASH_SOURCE%/*}/../.."

presto-product-tests/bin/run_on_docker.sh singlenode-sqlserver -g sqlserver
