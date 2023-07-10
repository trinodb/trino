# Delta Lake Connector Developer Notes

The Delta Lake connector can be used to interact with [Delta Lake](https://delta.io/) tables.

Trino has product tests in place for testing its compatibility with the 
following Delta Lake implementations:

- Delta Lake OSS
- Delta Lake Databricks


## Delta Lake OSS Product tests

Testing against Delta Lake OSS is quite straightforward by simply spinning up
the corresponding product test environment:

```
testing/bin/ptl env up --environment singlenode-delta-lake-oss
```


## Delta Lake Databricks Product tests

At the time of this writing, Databricks Delta Lake and OSS Delta Lake differ in functionality provided.

In order to setup a Databricks testing environment there are several steps to be performed.

### Delta Lake Databricks on AWS

Start by setting up a Databricks account via https://databricks.com/try-databricks and after
filling your contact details, choose *AWS* as preferred cloud provider.

Create an AWS S3 bucket to be used for storing the content of the Delta Lake tables managed
by the Databricks runtime.

Follow the guideline [Secure access to S3 buckets using instance profiles](https://docs.databricks.com/administration-guide/cloud-configurations/aws/instance-profiles.html)
for allowing the Databricks cluster to access the AWS S3 bucket on which the Delta Lake tables are stored or AWS Glue
table metastore.

In order to make sure that the setup has been done correctly, proceed via Databricks Web UI to
create a notebook on which a simple table could be created:

```
%sql
CREATE TABLE default.test1 ( 
    a_bigint BIGINT) 
USING DELTA LOCATION 's3://my-s3-bucket/test1'
```

### Use AWS Glue Data Catalog as the metastore for Databricks Runtime

[AWS Glue](https://aws.amazon.com/glue) is the metastore of choice for Databricks Delta Lake product tests
on Trino because it is a managed solution which allows connectivity to the metastore backing the
Databricks runtime from Trino as well while executing the product tests.

Follow the guideline [Use AWS Glue Data Catalog as the metastore for Databricks Runtime](https://docs.databricks.com/data/metastores/aws-glue-metastore.html#configure-glue-data-catalog-as-the-metastore)
for performing the setup of Glue as Data Catalog on your Databricks Cluster.

After performing successfully this step you should be able to perform any of the statements:

```
show databases;

show tables;
```

The output of the previously mentioned statements should be the same as the one seen on the
AWS Glue administration Web UI.


### Create AWS user to be used by Trino for managing Delta Lake tables

Trino needs a set of security credentials for successfully connecting to the AWS infrastructure
in order to perform create/drop tables on AWS Glue and read/modify table content on AWS S3.

Create via AWS IAM a user which has the appropriate policies for interacting with AWS.
Below are presented a set of simplistic permission policies which can be configured on this
user:

`GlueAccess`

```
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "GrantCatalogAccessToGlue",
      "Effect": "Allow",
      "Action": [
        "glue:BatchCreatePartition",
        "glue:BatchDeletePartition",
        "glue:BatchGetPartition",
        "glue:CreateDatabase",
        "glue:CreateTable",
        "glue:CreateUserDefinedFunction",
        "glue:DeleteDatabase",
        "glue:DeletePartition",
        "glue:DeleteTable",
        "glue:DeleteUserDefinedFunction",
        "glue:GetDatabase",
        "glue:GetDatabases",
        "glue:GetPartition",
        "glue:GetPartitions",
        "glue:GetTable",
        "glue:GetTables",
        "glue:GetUserDefinedFunction",
        "glue:GetUserDefinedFunctions",
        "glue:UpdateDatabase",
        "glue:UpdatePartition",
        "glue:UpdateTable",
        "glue:UpdateUserDefinedFunction"
      ],
      "Resource": [
        "*"
      ]
    }
  ]
}
```

`S3Access`

```
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Action": [
                "s3:GetBucketLocation",
                "s3:ListBucket"
            ],
            "Resource": [
                "arn:aws:s3:::my-s3-bucket"
            ]
        },
        {
            "Effect": "Allow",
            "Action": [
                "s3:PutObject",
                "s3:GetObject",
                "s3:DeleteObject",
                "s3:PutObjectAcl"
            ],
            "Resource": [
                "arn:aws:s3:::my-s3-bucket/*"
            ]
        }
    ]
}
```

For the newly created AWS IAM user, make sure to retrieve the security credentials because they
are to be used by Trino in communicating with AWS. 


### Setup token authentication on the Databricks cluster

Follow the guideline [Authentication using Databricks personal access tokens](https://docs.databricks.com/dev-tools/api/latest/authentication.html)
for setting up your Databricks personal access token.


### Test the functionality of the Databricks Delta Lake product test environment


Run the following command for spinning up the Databricks 9.1 Delta Lake product test
environment for Trino:

```
env S3_BUCKET=my-s3-bucket \
    AWS_REGION=us-east-2 \
    TRINO_AWS_SECRET_ACCESS_KEY=xxx \
    TRINO_AWS_ACCESS_KEY_ID=xxx \
    DATABRICKS_91_JDBC_URL='xxx' \
    DATABRICKS_LOGIN=token \
    DATABRICKS_TOKEN=xxx \
    testing/bin/ptl env up  --environment singlenode-delta-lake-databricks91
```
