# Redshift Connector

To run the Redshift tests you will need to provision a Redshift cluster.  The
tests are designed to run on the smallest possible Redshift cluster containing
is a single dc2.large instance. Additionally, you will need a S3 bucket 
containing TPCH tiny data in Parquet format.  The files should be named:

```
s3://<your_bucket>/tpch/tiny/<table_name>/*.parquet
```

To run the tests set the following system properties:

```
test.redshift.jdbc.endpoint=<your_endpoint>.<your_region>.redshift.amazonaws.com:5439/
test.redshift.jdbc.user=<username>
test.redshift.jdbc.password=<password>
test.redshift.s3.tpch.tables.root=<your_bucket>
test.redshift.iam.role=<your_iam_arm_to_access_bucket>
```

## Redshift Cluster CI Infrastructure setup

### AWS VPC setup
On _AWS VPC_ service create a VPC - `redshift-vpc`.
Key properties to configure on the VPC:

- `IPv4 CIDR`: `192.168.0.0/16`

Create for the `redshift-vpc` an Internet Gateway - `redshift-igw`.

Create a subnet for the VPC `redshift-public-subnet`.
In the route table of the subnet make sure to add the route 
`Destination 0.0.0.0/0` to `Target` the previously created 
internet gateway `redshift-igw`.

Create a Security Group `redshift-sg`.
Make the following adjustments in the security group to allow access to the 
Redshift cluster from the general purpose Github CI runners:

- add an Inbound rule accepting `All traffic` from Source `0.0.0.0/0`
- add an Outbound rule for `All traffic` to destination `0.0.0.0/0`

### Amazon Redshift setup

Create a subnet group `cluster-subnet-group-trino-ci` associated with 
the VPC `redshift-vpc` and the VPC subnet `redshift-public-subnet`.

### AWS IAM setup

Create the AWS IAM role `redshift-ci` and add to it 
the `AmazonRedshiftAllCommandsFullAccess` policy.
This role will be passed to the ephemeral Redshift cluster to provide it with 
the ability to execute `COPY` from AWS S3 bucket.

Ensure that the AWS IAM user used by the CI process does have the ability to 
create ephemeral Amazon Redshift clusters: 

```
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Sid": "PassRoleToRedshiftCluster",
            "Effect": "Allow",
            "Action": "iam:PassRole",
            "Resource": "arn:aws:iam::894365193301:role/redshift-ci"
        },
        {
            "Sid": "RedshiftClusterManagement",
            "Effect": "Allow",
            "Action": [
                "redshift:DeleteTags",
                "redshift:DeleteCluster",
                "redshift:CreateTags",
                "redshift:CreateCluster",
                "redshift:DescribeClusters",
                "redshift:DescribeLoggingStatus"
            ],
            "Resource": "arn:aws:redshift:us-east-2:894365193301:cluster:trino-redshift-ci-cluster-*"
        },
        {
            "Sid": "DescribeRedshiftVpcComponents",
            "Effect": "Allow",
            "Action": [
                "ec2:DescribeInternetGateways",
                "ec2:DescribeAddresses",
                "ec2:DescribeAvailabilityZones",
                "ec2:DescribeVpcs",
                "ec2:DescribeAccountAttributes",
                "ec2:DescribeSubnets",
                "ec2:DescribeSecurityGroups"
            ],
            "Resource": "*"
        }
    ]
}
```

### AWS Lambda setup

The test `io.trino.plugin.redshift.TestRedshiftConnectorTest.testCancellation` relies on an AWS Lambda function to simulate a long running query 
in Redshift and test the cancellation of it.
Below are the steps to create and configure the AWS Lambda function:

1. Create the function code

```
cat > lambda_function.py << 'EOF'                                                                                      
import json
import time

def lambda_handler(event, context):
    results = []
    for row_args in event['arguments']:
        time.sleep(row_args[0])
        results.append(1)
    return json.dumps({"results": results, "success": True})

EOF
```

2. Package it

```
zip lambda_function.zip lambda_function.py
```

3. Prepare the execution role for the function

```
aws iam create-role \
    --role-name trino-redshift-sleep-lambda-execution \
    --assume-role-policy-document '{                                                                                     
      "Version": "2012-10-17",                                                                                           
      "Statement": [{
        "Effect": "Allow",
        "Principal": {"Service": "lambda.amazonaws.com"},
        "Action": "sts:AssumeRole"
      }]
    }'
```

```
aws iam attach-role-policy \
    --role-name trino-redshift-sleep-lambda-execution \
    --policy-arn arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole 
```

4. Create the AWS lambda function

```
aws lambda create-function \
    --function-name trino-redshift-ci-sleep \
    --runtime python3.12 \
    --handler lambda_function.lambda_handler \
    --role arn:aws:iam::894365193301:role/trino-redshift-sleep-lambda-execution \
    --zip-file fileb://lambda_function.zip \
    --timeout 120 \
    --region us-east-2
```

5. Allow Redshift to invoke it:

```
aws lambda add-permission \
    --function-name trino-redshift-ci-sleep \
    --statement-id redshift-ci-invoke \
    --action lambda:InvokeFunction \
    --principal arn:aws:iam::894365193301:role/redshift-ci \
    --region us-east-2
```

```
aws iam put-role-policy \
    --role-name redshift-ci \
    --policy-name invoke-trino-ci-redshift-sleep \
    --policy-document '{                                                                                                 
      "Version": "2012-10-17",                                                                                         
      "Statement": [{
        "Effect": "Allow",
        "Action": "lambda:InvokeFunction",                                                                               
        "Resource": "arn:aws:lambda:us-east-2:894365193301:function:trino-redshift-ci-sleep"
      }]                                                                                                                 
    }'
```

### AWS S3 setup

The `trino-redshift` tests rely on a Redshift cluster 
having TPCH tables filled with data.
Create an AWS S3 bucket and add to it the parquet content 
of `tpch` tables saved locally through the `trino-hive` connector 
via commands like:

```
CREATE TABLE hive.tiny.table_name WITH (format= 'parquet') AS TABLE tpch.sf1.table_name
```

The content of the S3 bucket should look like this:

```
s3://<your_bucket>/tpch/tiny/<table_name>/*.parquet
```

where `table_name` is:

- `customer`
- `lineitem`
- `nation`
- `orders`
- `part`
- `partsupp`
- `region`
- `supplier`

