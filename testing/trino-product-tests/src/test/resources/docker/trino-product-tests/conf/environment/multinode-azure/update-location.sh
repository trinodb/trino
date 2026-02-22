#!/usr/bin/env bash

set -euxo pipefail

/usr/bin/mysqld_safe &
while ! mysqladmin ping -proot --silent; do sleep 1; done

hive --service metatool -updateLocation "abfs://${ABFS_CONTAINER}@${ABFS_ACCOUNT}.dfs.core.windows.net/${ABFS_SCHEMA}" hdfs://hadoop-master:9000/user/hive/warehouse

killall mariadbd || true
killall mysqld_safe || true
while pgrep -x mariadbd >/dev/null || pgrep -x mysqld_safe >/dev/null; do sleep 1; done
