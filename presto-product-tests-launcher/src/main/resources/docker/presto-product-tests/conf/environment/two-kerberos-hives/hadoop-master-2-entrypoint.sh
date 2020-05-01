#!/usr/bin/env bash

set -euo pipefail

cp /etc/presto/conf/hive-presto-master.keytab /presto_keytabs/other-hive-presto-master.keytab
cp /etc/presto/conf/presto-server.keytab /presto_keytabs/other-presto-server.keytab

supervisord -c /etc/supervisord.conf
