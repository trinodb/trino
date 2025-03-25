## Create script

Structure of table:
- MOR table with MDT enabled
- Record Level Index is enabled with a recordKey of id,name
- Secondary index created on teh column `price`
- Col stats index disabled
- Revision: eb212c9dca876824b6c570665951777a772bc463
- Written with master branch 
- 2 Partitions [US, SG]
- 2 Filegroups per partition

```scala
test("Create table multi filegroup partitioned mor") {
    withTempDir { tmp =>
        val tableName = "hudi_multi_fg_pt_rli_mor"
        spark.sql(
            // use composite key
            s"""
               |create table $tableName (
               |  id int,
               |  name string,
               |  price double,
               |  ts long,
               |  country string
               |) using hudi
               | location '${tmp.getCanonicalPath}'
               | tblproperties (
               |  primaryKey ='id,name',
               |  type = 'mor',
               |  preCombineField = 'ts'
               | ) partitioned by (country)
         """.stripMargin)
        // enable record level index
        spark.sql(s"set hoodie.metadata.record.index.enable=true")
        spark.sql(s"set hoodie.metadata.index.secondary.enable=true")
        // disable colstats index
        spark.sql(s"set hoodie.metadata.index.column.stats.enable=false")
        // directly write to new parquet file
        spark.sql(s"set hoodie.parquet.small.file.limit=0")
        spark.sql(s"set hoodie.metadata.compact.max.delta.commits=1")
        // 2 filegroups per partition
        spark.sql(s"insert into $tableName values(1, 'a1', 100, 1000, 'SG'),(2, 'a2', 101, 1000, 'US')")
        spark.sql(s"insert into $tableName values(3, 'a3', 102, 1001, 'SG'),(4, 'a3', 103, 1001, 'US')")
        // create secondary index
        spark.sql(s"create index idx_price on $tableName (price)")
        // generate logs through updates
        spark.sql(s"update $tableName set price=price+1")
    }
}
```

# When to use this table?
- For test cases that require multiple filegroups in a partition
- For test cases that require filegroups that have a log file
- For test cases that require row level index
- For test cases that require secondary index
