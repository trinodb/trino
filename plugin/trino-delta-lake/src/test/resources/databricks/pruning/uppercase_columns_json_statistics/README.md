Data generated using Databricks 7.3 :

```
%sql
CREATE TABLE manual_testing.uppercase_columns_statistics(BLAH BIGINT)
USING delta
LOCATION 's3://starburst-alex/delta/manual_testing/uppercase_columns_statistics';
```

```
%scala
val batches = List(
List(1L, 2L, 3L),
List(3L, 4L, 5L),
List(7L, 10L, 15L),
List(20L, 21L));

for (values <- batches) {
  values.toDF("BLAH").repartition(1).write.format("delta").mode("append").save("s3://starburst-alex/delta/manual_testing/uppercase_columns_statistics");  
}
```