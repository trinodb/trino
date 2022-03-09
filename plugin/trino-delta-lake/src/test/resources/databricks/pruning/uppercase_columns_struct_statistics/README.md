Data generated using Databricks 7.3 :

```
%sql
CREATE TABLE manual_testing.uppercase_columns_statistics_struct(BLAH BIGINT)
USING delta
LOCATION 's3://starburst-alex/delta/manual_testing/uppercase_columns_statistics_struct';
ALTER TABLE manual_testing.uppercase_columns_statistics_struct SET TBLPROPERTIES ( delta.checkpoint.writeStatsAsStruct = true, delta.checkpoint.writeStatsAsJson = false );
```

```
%scala
// 11 entries so *.checkpoint.parquet is created
val batches = List(
List(1L, 2L, 3L),
List(3L, 4L, 5L),  
List(7L, 10L, 15L),
List(100L),
List(100L),
List(100L),
List(100L),
List(100L),
List(100L),
List(100L),
List(100L),
);

for (values <- batches) {
  values.toDF("BLAH").repartition(1).write.format("delta").mode("append").save("s3://starburst-alex/delta/manual_testing/uppercase_columns_statistics_struct");  
}
```