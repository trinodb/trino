This file is prepared to have parquet page indexes and two small row-groups with many small pages per row-group.
It required using release <= 422 because the new Trino parquet writer does not support writing page indexes yet.

```sql
set session hive.parquet_writer_batch_size=10;
set session hive.parquet_writer_page_size='10Kb';
set session hive.parquet_writer_block_size='1MB';
set session hive.parquet_optimized_writer_enabled=false;

create table lineitem with (format='parquet', sorted_by=array['l_shipdate'], bucketed_by=array['l_shipdate'], bucket_count=1) as select * from tpch.tiny.lineitem;
```
