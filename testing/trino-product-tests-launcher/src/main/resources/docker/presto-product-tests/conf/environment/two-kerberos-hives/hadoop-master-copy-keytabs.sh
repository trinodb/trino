#!/usr/bin/env bash

set -euo pipefail

echo "Copying kerberos keytabs to /presto_keytabs/"
cp /etc/trino/conf/* /presto_keytabs/
