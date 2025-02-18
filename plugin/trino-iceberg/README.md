# Iceberg Connector Developer Notes

Steps to create TPCH tables on S3 Tables:
1. Set `AWS_REGION`, `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY` environment variables.
2. Replace placeholders in the following command and run it:
```sh
./spark-sql \
--packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.6.1,software.amazon.awssdk:bundle:2.20.10,software.amazon.s3tables:s3-tables-catalog-for-iceberg-runtime:0.1.3,org.apache.kyuubi:kyuubi-spark-connector-tpch_2.12:1.8.0 \
--conf spark.sql.catalog.s3tablesbucket=org.apache.iceberg.spark.SparkCatalog \
--conf spark.sql.catalog.s3tablesbucket.catalog-impl=software.amazon.s3tables.iceberg.S3TablesCatalog \
--conf spark.sql.catalog.s3tablesbucket.warehouse=arn:aws:s3tables:{region}:{account-id}:bucket/{bucket-name} \
--conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
--conf spark.sql.catalog.tpch=org.apache.kyuubi.spark.connector.tpch.TPCHCatalog
```

3. Run the following command to create TPCH tables:
```sql
CREATE TABLE s3tablesbucket.tpch.nation AS SELECT 
  n_nationkey AS nationkey,
  n_name AS name,
  n_regionkey AS regionkey,
  n_comment AS comment
FROM tpch.tiny.nation;

CREATE TABLE s3tablesbucket.tpch.region AS SELECT 
  r_regionkey AS regionkey, 
  r_name AS name, 
  r_comment AS comment 
FROM tpch.tiny.region;
```
