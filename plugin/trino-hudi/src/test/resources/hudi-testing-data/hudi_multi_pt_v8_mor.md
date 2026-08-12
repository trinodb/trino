## Create script
Revision: 6f65998117a2d1228fc96d36053bd0d394499afe

```scala
test("Create MOR table with multiple partition fields with multiple types") {
  withTempDir { tmp =>
    val tableName = "hudi_multi_pt_v8_mor"
    // Save current session timezone and set to UTC for consistency in test
    val originalTimeZone = spark.conf.get("spark.sql.session.timeZone")
    spark.conf.set("spark.sql.session.timeZone", "UTC")

    spark.sql(
      s"""
         |CREATE TABLE $tableName (
         |  id INT,
         |  name STRING,
         |  price DOUBLE,
         |  ts LONG,
         |  -- Partition Fields --
         |  part_str STRING,
         |  part_int INT,
         |  part_date DATE,
         |  part_bigint BIGINT,
         |  part_decimal DECIMAL(10,2),
         |  part_timestamp TIMESTAMP,
         |  part_bool BOOLEAN
         |) USING hudi
         | LOCATION '${tmp.getCanonicalPath}'
         | TBLPROPERTIES (
         |  primaryKey = 'id,name',
         |  type = 'mor',
         |  preCombineField = 'ts'
         | )
         | PARTITIONED BY (part_str, part_int, part_date, part_bigint, part_decimal, part_timestamp, part_bool)
     """.stripMargin)

    // Configure Hudi properties
    spark.sql(s"SET hoodie.parquet.small.file.limit=0") // Write to a new parquet file for each commit
    spark.sql(s"SET hoodie.metadata.compact.max.delta.commits=1")
    spark.sql(s"SET hoodie.metadata.enable=true")
    spark.sql(s"SET hoodie.metadata.index.column.stats.enable=true")
    spark.sql(s"SET hoodie.compact.inline.max.delta.commits=9999") // Disable compaction plan trigger

    // Insert data with new partition values
    spark.sql(s"INSERT INTO $tableName VALUES(1, 'a1', 100.0, 1000, 'books', 2023, date'2023-01-15', 10000000001L, decimal('123.45'), timestamp'2023-01-15 10:00:00.123', true)")
    spark.sql(s"INSERT INTO $tableName VALUES(2, 'a2', 200.0, 1000, 'electronics', 2023, date'2023-03-10', 10000000002L, decimal('50.20'), timestamp'2023-03-10 12:30:00.000', false)")
    spark.sql(s"INSERT INTO $tableName VALUES(3, 'a3', 101.0, 1001, 'books', 2024, date'2024-02-20', 10000000003L, decimal('75.00'), timestamp'2024-02-20 08:45:10.456', true)")
    spark.sql(s"INSERT INTO $tableName VALUES(4, 'a4', 201.0, 1001, 'electronics', 2023, date'2023-03-10', 10000000002L, decimal('50.20'), timestamp'2023-03-10 12:30:00.000', true)") // Same as record 2 part except boolean
    spark.sql(s"INSERT INTO $tableName VALUES(5, 'a5', 300.0, 1002, 'apparel', 2024, date'2024-01-05', 20000000001L, decimal('99.99'), timestamp'2024-01-05 18:00:00.789', false)")

    // Generate logs through updates
    spark.sql(s"UPDATE $tableName SET price = price + 2.0 WHERE part_bool = true AND part_str = 'books'")
    spark.sql(s"UPDATE $tableName SET price = ROUND(price * 1.02, 2) WHERE part_bigint = 10000000002L")
  }
}
```
