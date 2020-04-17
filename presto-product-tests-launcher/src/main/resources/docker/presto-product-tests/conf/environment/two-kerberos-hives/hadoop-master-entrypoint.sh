#!/usr/bin/env bash

set -euo pipefail

cp /etc/presto/conf/* /presto_keytabs/

supervisord -c /etc/supervisord.conf
