Data generated using OSS DELTA 2.0.0:

```sql
CREATE TABLE default.uri_test (part string, y long)
USING delta
PARTITIONED BY (part)
LOCATION '/home/username/trino/plugin/trino-delta-lake/src/test/resources/databricks/uri';

INSERT INTO default.uri_test VALUES ('a=equal', 1);
INSERT INTO default.uri_test VALUES ('a:colon', 2);
INSERT INTO default.uri_test VALUES ('a+plus', 3);
INSERT INTO default.uri_test VALUES ('a space', 4);
INSERT INTO default.uri_test VALUES ('a%percent', 5);
```
