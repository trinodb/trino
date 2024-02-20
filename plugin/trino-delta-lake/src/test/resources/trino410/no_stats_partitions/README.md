Data generated using Trino:

```sql
CREATE TABLE no_stats_partitions (p_str, c_int, c_str) WITH (partitioned_by = ARRAY['p_str'])AS VALUES ('p?p', 42, 'foo'), ('p?p', 12, 'ab'), (null, null, null);
INSERT INTO no_stats_partitions VALUES ('ppp', 15,'cd'), ('ppp', 15,'bar');
```

with removed:

- stats entries from json files
- json crc files
- _trino_metadata directory
