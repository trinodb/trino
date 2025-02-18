Data generated using OSS Delta Lake 3.3.0:

```sql
CREATE TABLE test_in_commit_history_read
(id int, v int)
USING DELTA;

INSERT INTO test_in_commit_history_read VALUES (1, 1);

ALTER TABLE test_in_commit_history_read 
    SET TBLPROPERTIES ('delta.enableInCommitTimestamps' = 'true');

INSERT INTO test_in_commit_history_read VALUES (5, 5);
```
