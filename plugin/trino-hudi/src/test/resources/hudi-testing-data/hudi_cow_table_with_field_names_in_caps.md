## Create script

Structure of table:
- COW table with field names containing caps
- No log files

```scala
import org.apache.spark.sql.types._
import org.apache.spark.sql.Row

val schema = StructType(Seq(
    StructField("Id", StringType, nullable = false),
    StructField("Name", StringType, nullable = true),
    StructField("Age", IntegerType, nullable = true)
))


val data = Seq(
    Row("1", "Alice", 30),
    Row("2", "Bob", 25)
)

val df = spark.createDataFrame(
    spark.sparkContext.parallelize(data),
    schema
)

df.show()

var basePath = "file:///tmp/hudi_cow_table_with_field_names_in_caps/"

df.write.format("hudi").mode("Append")
        .option("hoodie.table.name", "hudi_cow_table_with_field_names_in_caps")
        .option("hoodie.datasource.write.table.type", "COPY_ON_WRITE")
        .option("hoodie.datasource.write.recordkey.field","Id")
        .option("hoodie.datasource.write.operation","bulk_insert")
        .option("hoodie.metadata.index.column.stats.enable", "true")
        .option("hoodie.metadata.record.index.enable", "true")
        .option("hoodie.datasource.write.secondarykey.column", "Name")
        .save(basePath)
```
