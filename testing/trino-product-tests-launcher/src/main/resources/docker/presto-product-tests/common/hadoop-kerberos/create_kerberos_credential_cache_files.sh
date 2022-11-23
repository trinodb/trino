#!/usr/bin/env bash

set -exuo pipefail

kinit -f -c /etc/trino/conf/presto-server-krbcc \
      -kt /etc/trino/conf/presto-server.keytab presto-server/$(hostname -f)@LABS.TERADATA.COM

kinit -f -c /etc/trino/conf/hive-presto-master-krbcc \
      -kt /etc/trino/conf/hive-presto-master.keytab hive/$(hostname -f)@LABS.TERADATA.COM


kinit -f -c /etc/trino/conf/hdfs-krbcc \
      -kt /etc/hadoop/conf/hdfs.keytab hdfs/hadoop-master@LABS.TERADATA.COM

kinit -f -c /etc/trino/conf/hive-krbcc \
      -kt /etc/hive/conf/hive.keytab hive/hadoop-master@LABS.TERADATA.COM
