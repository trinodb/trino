## Snowflake Iceberg Catalog setup guide

This guide will walk through the Snowflake Iceberg catalog with S3 as an underlying storage created for testing the Iceberg Snowflake catalog in Trino.
While writing this setup guide, Snowflake Iceberg catalog has a read-only support and hence Iceberg Snowflake catalog in Trino also has a read-only support.
For testing purpose, some of the TPCH tables are created through tests by copying the data from pre-existing `SNOWFLAKE_SAMPLE_DATA` Snowflake database.

Iceberg Snowflake catalog creation steps are taken from [official Iceberg Snowflake catalog guide](https://quickstarts.snowflake.com/guide/getting_started_iceberg_tables/index.html?_ga=2.81143831.1840596713.1702066099-371582571.1622222327#2)

Below are the steps to create an external volume as outline in [external volume creation official guide](https://docs.snowflake.com/en/user-guide/tables-iceberg-configure-external-volume#label-tables-iceberg-configure-external-volume-s3-create-role)

1. Create an S3 bucket.  

2. Create an IAM policy with having [required access](https://docs.snowflake.com/en/user-guide/tables-iceberg-configure-external-volume#aws-access-control-requirements) to the created bucket.  

3. Create a [cross account IAM role](https://docs.snowflake.com/en/user-guide/tables-iceberg-configure-external-volume#step-2-create-an-iam-role-in-aws) to be used in external
   volume creation.  

4. Create an [external volume](https://docs.snowflake.com/en/user-guide/tables-iceberg-configure-external-volume#step-4-create-an-external-volume-in-snowflake) in Snowflake.  

5. Retrieve the Snowflake AWS IAM user and external id from the created volume.
   Collect the value of `STORAGE_AWS_IAM_USER_ARN`(Snowflake IAM user) and `STORAGE_AWS_EXTERNAL_ID`(Snowflake external id).

6. [Update the trust relationship](https://docs.snowflake.com/en/user-guide/tables-iceberg-configure-external-volume#step-6-grant-the-iam-user-permissions-to-access-bucket-objects)
   of the IAM role created in step #3.  

Now the external volume is created so the Iceberg Snowflake catalog setup is complete. Iceberg tables can be created in Snowflake using the created volume. Given below the
additional step required for setting up testing S3 catalog in Trino.

- Create AWS IAM user with `s3:GetObject` access to the same S3 bucket(used in creating a Snowflake external volume). User requires only `s3:GetObject` access as the Iceberg 
   Snowflake catalog has a read-only support. IAM user credentials to be used for creating Trino S3 catalog.  
