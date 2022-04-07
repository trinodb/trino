These metadata files were generated from the hive connector from TPCDS/TPCH schema with sf1000.
So, there are no min/max statistics for char-based columns as hive connector doesn't support them yet.

## How to generate these metadata files?

### Prerequisites
1. You should have a running trino cluster with configure hive (or glue) connector.
2. You should have tpch/tpcds unpartitioned data in your hive catalog.
    - The tpch tables should be under `tpch_sf1000_orc` schema.
    - The tpcds tables should be under `tpcds_sf1000_orc` schema.
3. To create tpch/tpcds data, you could use builtin TPCH/TPCDS connectors using `CREATE TABLE AS SELECT` (CTAS) command.
4. If you want to use some other schemas with different scale factor, you can configure it in
   `HiveMetadataRecorder.UpdateMetadataFiles.main()`.

### Generating files
1. If you are using glue metastore, then you need to be logged into your AWS account. For example if you use 
   MFA authentication, you can use`gimme-aws-creds --mfa-code <code>`.
2. Otherwise, for normal hive metastore, you can set the appropriate config in `test/resources/hive.properties`.
2. Run `HiveMetadataRecorder.UpdateMetadataFiles.main()`
