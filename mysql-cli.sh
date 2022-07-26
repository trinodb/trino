#!/usr/bin/env bash

set -xeuo pipefail

docker-compose -f debezium-mysql.yaml exec mysql bash -c 'exec mysql -u $MYSQL_USER -p$MYSQL_PASSWORD'
