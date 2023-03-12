# Redshift Connector

To run the Redshift tests you will need to provision a Redshift cluster.  The
tests are designed to run on the smallest possible Redshift cluster containing
is a single dc2.large instance. Additionally, you will need a S3 bucket 
containing TPCH tiny data in Parquet format.  The files should be named:

```
s3://<your_bucket>/tpch/tiny/<table_name>.parquet
```

To run the tests set the following system properties:

```
test.redshift.jdbc.endpoint=<your_endpoint>.<your_region>.redshift.amazonaws.com:5439/
test.redshift.jdbc.user=<username>
test.redshift.jdbc.password=<password>
test.redshift.s3.tpch.tables.root=<your_bucket>
test.redshift.iam.role=<your_iam_arm_to_access_bucket>
```
