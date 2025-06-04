Data generated using OSS Delta Lake 3.3.0:

```sql
CREATE TABLE clone_merge_cloned SHALLOW CLONE clone_merge_source;
```

Note the `path` in add/remove entry in `_delta_log/00000000000000000000.json` is manually modified
that the prefix is changed to `../clone_merge_source` for test purpose, i.e, the original `path` in add entry value like 
`s3://test-bucket/tiny/clone_merge_source/part=2024-02-02/part-00002-819d3304-6591-4d7f-87e3-2a9683686644.c000.snappy.parquet` 
is changed to `../clone_merge_source/part=2024-02-02/part-00002-819d3304-6591-4d7f-87e3-2a9683686644.c000.snappy.parquet`.
