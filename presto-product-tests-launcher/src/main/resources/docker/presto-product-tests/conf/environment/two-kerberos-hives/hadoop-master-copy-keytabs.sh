#!/usr/bin/env bash

set -euo pipefail

echo "Copying kerberos keytabs to /presto_keytabs/"
cp /etc/presto/conf/* /presto_keytabs/
