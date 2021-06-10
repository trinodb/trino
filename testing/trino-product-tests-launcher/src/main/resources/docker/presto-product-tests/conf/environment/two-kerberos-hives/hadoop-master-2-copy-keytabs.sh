#!/usr/bin/env bash

set -euo pipefail

echo "Copying kerberos keytabs to /presto_keytabs/"
cp /etc/trino/conf/hive-presto-master.keytab /presto_keytabs/other-hive-presto-master.keytab
cp /etc/trino/conf/presto-server.keytab /presto_keytabs/other-presto-server.keytab
