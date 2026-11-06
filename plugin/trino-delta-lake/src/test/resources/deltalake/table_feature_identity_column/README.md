Data generated using Spark 4.1.0 and Delta Lake 4.4.0:

```sql
CREATE TABLE delta.`/data/table_feature_identity_column`
(a INT, b BIGINT GENERATED ALWAYS AS IDENTITY)
USING DELTA
TBLPROPERTIES ('delta.feature.identityColumns' = 'supported');
INSERT INTO delta.`/data/table_feature_identity_column` (a) VALUES (1), (2);
```
