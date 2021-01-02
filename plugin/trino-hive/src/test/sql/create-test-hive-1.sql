-- ALTER TABLE .. ENABLE OFFLINE was removed in Hive 2.0
ALTER TABLE trino_test_offline_partition PARTITION (ds='2012-12-30') ENABLE OFFLINE;
