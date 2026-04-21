Data generated using OSS Delta Lake 3.3.0:

```sql
CREATE TABLE clone_merge_source
(id int, v string, part date) 
USING DELTA 
LOCATION ?
PARTITIONED BY (part);

INSERT INTO clone_merge_source 
VALUES 
    (1, 'A', TIMESTAMP '2024-01-01'),
    (2, 'B', TIMESTAMP '2024-01-01'),
    (3, 'C', TIMESTAMP '2024-02-02'),
    (4, 'D', TIMESTAMP '2024-02-02');
```
