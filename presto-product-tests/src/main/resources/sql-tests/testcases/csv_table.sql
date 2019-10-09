-- database: presto; tables: csv_table; groups: hive, storage_formats;
--! name: scan from csv table
SELECT * FROM csv_table
--!
10|""
--! name: describe csv table
DESCRIBE csv_table
--!
c_bigint|varchar|||
c_varchar|varchar|||
--! name: show create csv table
SHOW CREATE TABLE csv_table
--!
CREATE TABLE hive.default.csv_table (\n   c_bigint varchar,\n   c_varchar varchar\n)\nWITH (\n   external = true,\n   format = 'CSV',\n   location = 'hdfs://hadoop-master:9000/tmp/product-test/datasets/csv_table'\n)
--! name: create table like
DROP TABLE IF EXISTS like_csv_table;
CREATE TABLE like_csv_table (LIKE csv_table INCLUDING PROPERTIES);
SHOW CREATE TABLE like_csv_table
--!
CREATE TABLE hive.default.like_csv_table (\n   c_bigint varchar,\n   c_varchar varchar\n)\nWITH (\n   external = false,\n   format = 'CSV',\n   location = 'hdfs://hadoop-master:9000/user/hive/warehouse/like_csv_table'\n)
