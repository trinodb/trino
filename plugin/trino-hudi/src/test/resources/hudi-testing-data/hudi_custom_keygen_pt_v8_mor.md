## Create script

Structure of table:
- MOR table with CustomKeyGenerator
- Revision: f7157954a9819137446d8e3a1d331d003f069414
- No log files


```scala
test("Create MOR table with custom keygen partition field") {
    withTempDir { tmp =>
        val tableName = "hudi_custom_keygen_pt_v8_mor"

        spark.sql(
            s"""
               |CREATE TABLE $tableName (
               |  id INT,
               |  name STRING,
               |  price DOUBLE,
               |  ts LONG,
               |  -- Partition Source Fields --
               |  partition_field_country STRING,
               |  partition_field_date BIGINT
               |) USING hudi
               | LOCATION '${tmp.getCanonicalPath}'
               | TBLPROPERTIES (
               |  primaryKey = 'id',
               |  type = 'mor',
               |  preCombineField = 'ts',
               |  -- Timestamp Keygen and Partition Configs --
               |  hoodie.table.keygenerator.class = 'org.apache.hudi.keygen.CustomKeyGenerator',
               |  hoodie.datasource.write.partitionpath.field = 'partition_field_country:SIMPLE,partition_field_date:TIMESTAMP',
               |  hoodie.keygen.timebased.timestamp.type = 'EPOCHMILLISECONDS',
               |  hoodie.keygen.timebased.output.dateformat = 'yyyy-MM-dd',
               |  hoodie.keygen.timebased.timezone = 'UTC'
               | ) PARTITIONED BY (partition_field_country, partition_field_date)
     """.stripMargin)

        // To not trigger compaction scheduling, and compaction
        spark.sql(s"set hoodie.compact.inline.max.delta.commits=9999")
        spark.sql(s"set hoodie.compact.inline=false")

        // Configure Hudi properties
        spark.sql(s"SET hoodie.metadata.enable=true")
        spark.sql(s"SET hoodie.metadata.index.column.stats.enable=true")

        // Insert data with new partition values
        spark.sql(s"INSERT INTO $tableName VALUES(1, 'a1', 100.0, 1000, 'SG', 1749284360000)")
        spark.sql(s"INSERT INTO $tableName VALUES(2, 'a2', 200.0, 1000, 'SG', 1749204000000)")
        spark.sql(s"INSERT INTO $tableName VALUES(3, 'a3', 101.0, 1001, 'US', 1749202000000)")
        spark.sql(s"INSERT INTO $tableName VALUES(4, 'a4', 201.0, 1001, 'CN', 1749102000000)")
        spark.sql(s"INSERT INTO $tableName VALUES(5, 'a5', 300.0, 1002, 'MY', 1747102000000)")
        spark.sql(s"INSERT INTO $tableName VALUES(6, 'a6', 301.0, 1000, 'SG', 1749284360000)")
        spark.sql(s"INSERT INTO $tableName VALUES(7, 'a7', 401.0, 1000, 'SG', 1749204000000)")

        // Generate logs through updates
        // NOTE: The query below will throw an error
        // spark.sql(s"UPDATE $tableName SET price = ROUND(price * 1.02, 2)")

        // NOTE: The query below will throw an error
        // spark.sql(s"SELECT * FROM $tableName").show(false)
    }
}
```
