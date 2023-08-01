Data generated using OSS Delta Lake:

```sql
CREATE TABLE default.test
(UPPER_CASE INT, PART INT)
USING delta
PARTITIONED BY (PART)
LOCATION 's3://trino-ci-test/test'
```
