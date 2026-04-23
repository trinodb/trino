## Create script

Structure of table:
- COW partitioned table with field names containing caps
- No log files

```scala
import org.apache.spark.sql.types._
import org.apache.spark.sql.Row

val schema = StructType(Seq(
    StructField("Id", StringType, nullable = false),
    StructField("Name", StringType, nullable = true),
    StructField("Age", IntegerType, nullable = true),
    StructField("Country", StringType, nullable = true)
))


var data = Seq(
    Row("1", "Alice", 30, "IND"),
    Row("2", "Bob", 25, "US")
)

var df = spark.createDataFrame(
    spark.sparkContext.parallelize(data),
    schema
)

df.show()

var basePath = "file:///tmp/hudi_cow_pt_table_with_field_names_in_caps/"

df.write.format("hudi").mode("Append").option("hoodie.table.name", "hudi_cow_pt_table_with_field_names_in_caps").option("hoodie.datasource.write.table.type", "COPY_ON_WRITE").option("hoodie.datasource.write.recordkey.field","Id").option("hoodie.datasource.write.operation","bulk_insert").option("hoodie.metadata.index.column.stats.enable", "true").option("hoodie.metadata.record.index.enable", "true").option("hoodie.datasource.write.secondarykey.column", "Name").option("hoodie.datasource.write.partitionpath.field", "Country").save(basePath)

data = Seq(
    Row("3", "Charlie", 30, "IND"),
    Row("4", "David", 25, "US")
)

df = spark.createDataFrame(
    spark.sparkContext.parallelize(data),
    schema
)

df.show()

df.write.format("hudi").mode("Append")
        .option("hoodie.table.name", "hudi_cow_pt_table_with_field_names_in_caps")
        .option("hoodie.datasource.write.table.type", "COPY_ON_WRITE")
        .option("hoodie.datasource.write.recordkey.field","Id")
        .option("hoodie.datasource.write.operation","bulk_insert")
        .option("hoodie.metadata.index.column.stats.enable", "true")
        .option("hoodie.metadata.record.index.enable", "true")
        .option("hoodie.datasource.write.secondarykey.column", "Name")
        .option("hoodie.datasource.write.partitionpath.field", "Country")
        .save(basePath)
```
