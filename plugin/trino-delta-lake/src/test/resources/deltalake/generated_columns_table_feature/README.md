Data generated using OSS Delta Lake 3.2.0:

```sql
CREATE TABLE default.generated_columns_table_feature
(a int, b int NOT NULL) 
USING DELTA 
LOCATION ?
TBLPROPERTIES ('delta.feature.generatedColumns' = 'supported');
```
