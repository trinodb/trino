Data generated using Apache Spark 4.1.0 & Delta Lake OSS 4.4.0

Delta Lake creates a table without columns when `CREATE TABLE` has no column list, because
`spark.databricks.delta.createEmptySchemaTable.enabled` defaults to true.

```sql
CREATE TABLE default.no_columns USING DELTA;
```
