This single-file Parquet table is a subset of the columns of
`tpch.tiny.customers`. It contains only the custkey, mktsegment,
and phone columns.

Its data is split into 15 row groups, each consisting of 100 rows.
The minima and maxima of the groups' custkeys are as follows:

```
Group  Min   Max   Omin Omax
   0     1   465     0    0
   1     8  1130     1    8
   2    94  1209     2    9
   3   115  1330     3   11
   4  1210  1395    13   12
   5   745  1302    11   10
   6   559   837     9    2
   7   608   893    10    3
   8   894  1086    12    6
   9   280  1116     5    7
  10   350  1007     6    4
  11   466  1023     7    5
  12   550   742     8    1
  13   187  1400     4   13
  14  1401  1500    14   14
```
(`Omin` and `Omax` are the indices when sorted by min and max,
respectively.)

Some interesting or useful statistics:
- 1 group contains the rows from 1401 to 1500.
- 1 group contains no data above 500
- 4 groups are completely between 500 and 1100
- 9 row groups have data between 1000 and 1100


# Creation

This table was created from Trino using the following statements:
```sql
SET SESSION delta.parquet_writer_block_size = '500B';

CREATE TABLE delta.default.custkey_15rowgroups
WITH (location = 's3://TEST_BUCKET/custkey_15rowgroups')
AS SELECT custkey, mktsegment, phone
FROM tpch.tiny.customer;
```
Note that re-creating this table will result in different row
groups. For that reason, the Parquet files should be copied each
time the table is needed.
