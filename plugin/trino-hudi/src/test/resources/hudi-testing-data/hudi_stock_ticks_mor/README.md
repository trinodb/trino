# Generating `hudi_stock_ticks_mor` table

`hudi_stock_ticks_mor` is a partitioned Merge-on-Read (MOR) table. The table
data is generated following the steps for
Hudi's [docker demo](https://hudi.apache.org/docs/docker_demo) using Hudi 
version 1.0.1.

The table has the following data columns:

| Name   | Type    |
|--------|---------|
| volume | bigint  |
| ts     | varchar |
| symbol | varchar |
| year   | integer |
| month  | varchar |
| high   | double  |
| low    | double  |
| key    | varchar |
| date   | varchar |
| close  | double  |
| open   | double  |
| day    | varchar |

The table is partitioned by `date`, and has a single partition at the
subdirectory `2018/08/31` with multiple file slices (an insert followed by 
update).
