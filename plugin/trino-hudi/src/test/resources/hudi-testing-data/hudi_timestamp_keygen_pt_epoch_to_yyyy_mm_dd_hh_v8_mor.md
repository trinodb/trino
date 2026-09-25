## Create script

Structure of table:
- MOR table with MDT enabled
- Timestamp key generator used (EPOCHMILLISECONDS -> yyyy-mm-dd)
- Hive style partitioning disabled
- Revision: 444cac26cb1077fd2b7deefc7b3713bacb270f9c

```scala
test("Create MOR table with timestamp keygen partition field (EPOCHMILLISECONDS -> yyyy-mm-dd hh)") {
withTempDir { tmp =>
val tableName = "hudi_timestamp_keygen_pt_epoch_to_yyyy_mm_dd_hh_v8_mor"

      spark.sql(
        s"""
           |CREATE TABLE $tableName (
           |  id INT,
           |  name STRING,
           |  price DOUBLE,
           |  ts LONG,
           |  -- Partition Source Fields --
           |  partition_field bigint
           |) USING hudi
           | LOCATION '${tmp.getCanonicalPath}'
           | TBLPROPERTIES (
           |  primaryKey = 'id',
           |  type = 'mor',
           |  preCombineField = 'ts',
           |  -- Hive style partitioning needs to be disabled for timestamp keygen to work --
           |  hoodie.datasource.write.hive_style_partitioning = 'false',
           |  -- Timestamp Keygen and Partition Configs --
           |  hoodie.table.keygenerator.class = 'org.apache.hudi.keygen.TimestampBasedKeyGenerator',
           |  hoodie.datasource.write.partitionpath.field = 'partition_field',
           |  hoodie.keygen.timebased.timestamp.type = 'EPOCHMILLISECONDS',
           |  hoodie.keygen.timebased.output.dateformat = 'yyyy-MM-dd hh',
           |  hoodie.keygen.timebased.timezone = 'UTC'
           | ) PARTITIONED BY (partition_field)
     """.stripMargin)

      // To not trigger compaction scheduling, and compaction
      spark.sql(s"set hoodie.compact.inline.max.delta.commits=9999")
      spark.sql(s"set hoodie.compact.inline=false")

      // Configure Hudi properties
      spark.sql(s"SET hoodie.parquet.small.file.limit=0") // Write to a new parquet file for each commit
      spark.sql(s"SET hoodie.metadata.compact.max.delta.commits=1")
      spark.sql(s"SET hoodie.metadata.enable=true")
      spark.sql(s"SET hoodie.metadata.index.column.stats.enable=true")

      // Insert data with new partition values
      spark.sql(s"INSERT INTO $tableName VALUES(1, 'a1', 100.0, 1000, 1749284360000L)")
      spark.sql(s"INSERT INTO $tableName VALUES(2, 'a2', 200.0, 1000, 1749204000000L)")
      spark.sql(s"INSERT INTO $tableName VALUES(3, 'a3', 101.0, 1001, 1749202000000L)")
      spark.sql(s"INSERT INTO $tableName VALUES(4, 'a4', 201.0, 1001, 1749102000000L)")
      spark.sql(s"INSERT INTO $tableName VALUES(5, 'a5', 300.0, 1002, 1747102000000L)")

      // Generate logs through updates
      spark.sql(s"UPDATE $tableName SET price = ROUND(price * 1.02, 2)")

      spark.sql(s"SELECT * FROM $tableName").show(false)
    }
}
```
