Data generated using Spark 4.1.0 and Delta Lake 4.4.0:

```sql
CREATE TABLE delta.`/data/table_feature_generated_column`
(a INT, b INT GENERATED ALWAYS AS (a * 2))
USING DELTA
TBLPROPERTIES ('delta.feature.generatedColumns' = 'supported');
INSERT INTO delta.`/data/table_feature_generated_column` (a) VALUES (1), (2);
```
