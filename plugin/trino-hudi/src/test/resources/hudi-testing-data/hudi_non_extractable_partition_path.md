## Create script

Structure of table:
- COW table with partition columns that Hudi’s partition extractor is unable to parse. 
- No log files

```scala
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.Row

val schema = StructType(Seq(
    StructField("id",   LongType,   nullable = false),
    StructField("name", StringType, nullable = true),
    StructField("ts",   LongType,   nullable = true),
    StructField("dt",   StringType, nullable = false),
    StructField("hh", StringType, nullable = false)
))


val data = Seq(
    Row(1L, "Alice", 1723272000000L, "2018-10-05", "10"),
    Row(2L, "Bob",   1723358400000L, "2018-10-05", "10"),
    Row(3L, "Charlie", 1723444800000L, "2018-10-06", "5"),
    Row(4L, "David", 1723531200000L, "2018-10-07", "5")
)

val dfRaw = spark.createDataFrame(
    spark.sparkContext.parallelize(data),
    schema
)


val df = dfRaw.withColumn("dt", date_format(to_date($"dt", "yyyy-MM-dd"), "yyyy/MM/dd"))

var basePath = "file:///tmp/hudi_non_extractable_partition_path"

df.write
        .format("hudi")
        .option("hoodie.table.name", "hudi_non_extractable_partition_path")
        .option("hoodie.datasource.write.recordkey.field", "id")
        .option("hoodie.datasource.write.partitionpath.field", "dt,hh")
        .option("hoodie.datasource.write.hive_style_partitioning", "false")
        .option("hoodie.datasource.write.operation", "insert")
        .mode(SaveMode.Overwrite)
        .save(basePath)
```
