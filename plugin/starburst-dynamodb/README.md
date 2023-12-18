# Starburst Enterprise DynamoDB Connector

Rather than using DynamoDB's native APIs and having a lot of code to write and maintain,
we instead leverage CData's DynamoDB JDBC Driver to abstract away the complexity of communicating with DynamoDB.
This leads to easier integration, however there are some quirks with the driver that developers should be aware of which is detailed below. 

## About the CData JDBC Driver

### Overview

The full documentation for the CData JDBC Driver for DynamoDB can be found at [CData's website here](https://cdn.cdata.com/help/DDF/jdbc/default.htm).

### Licensing

Starburst has an OEM contract with CData to use their drivers.
Part of this process was providing CData with a class or wildcarded package that will always be on the JVM stack when interacting with the CData driver.
CData built and provided us with a JAR file that includes this wildcarded package (`com.starburstdata.*`).
All drivers moving forward will have this built in, so we shouldn't need to worry about this.
If at any time the package name changes, we'd need to email `oemsupport@cdata.com` with the new package name so they can send us a new driver.
It typically takes 5 business days for them to provide an updated driver.

In addition to the wildcarded package, CData also gave us an OEM key which we add as a JDBC connection property.
This, combined with the driver, allows us to use any CData JDBC driver on any machine without any expiration date.
The OEM key should be held strictly private to Starburst and should not be given out to users.
The OEM key is hardcoded in `DynamoDbConnectionFactory` in case it ever needs updated.

### Support

CData provides support for their drivers.
Email oemsupport@cdata.com to open a ticket with them for any support issues or general questions about the drivers.

### Deploying JDBC Driver to Starburst Maven

CData delivers the OEM drivers to their customers via their Subscription Manager portal or will provide download links via email.
Developers should create an account using their email address and the below Subscription ID at https://www.cdata.com/subscriptions/manage/login.aspx.

Subscription ID for Starburst: `XSURB-VAENT-P3ZNR-2151A-TS8H6`

You can then click on the "Included Products" tab which has all of their drivers available for download.
They provide an executable`setup.jar` file to install the JAR file on your machine.
CData doesn't seem to have any versioning to their drivers besides the year, e.g. 2020.
As we get new JARs for a particular year, we can increment the suffixed version for the Maven artifact version.
View the `pom.xml` file for what version that is currently in use, then deploy the next version using the below command as an example.

```bash
# Authenticate to AWS starburstdata-sep-cicd account (843985043183) and the run the following

export CODEARTIFACT_AUTH_TOKEN=$(aws codeartifact get-authorization-token \
    --domain starburstdata-sep-cicd \
    --domain-owner 843985043183 \
    --query authorizationToken \
    --output text \
    --region us-east-2)

mvn deploy:deploy-file \
-Dfile=/path/to/cdata-dynamodb-jdbc.jar \
-DgroupId=com.cdata \
-DartifactId=dynamodb-jdbc \
-Dversion=2020-4 \
-Dpackaging=jar \
-DrepositoryId=starburstdata.releases \
-Durl=https://starburstdata-sep-cicd-843985043183.d.codeartifact.us-east-2.amazonaws.com/maven/releases/
```

## CData Metadata Cache

CData drivers automatically cache metadata about tables and columns from DynamoDB.
This metadata is not refreshed whenever a new table is created, so inserting data after creating a table may fail.
You can reset the metadata cache by executing `RESET SCHEMA CACHE` on the connection.
CData keeps a cache per distinct set of JDBC connection properties, so if you are using a separate connection
you will need to make sure you provide the same set of properties.
The `DynamoDbJdbcClient` will invalidate the cache after a table is created.

## Data Type Mappings

[CData documentation](https://cdn.cdata.com/help/DDF/jdbc/pg_dynamodbcolumns.htm) for type mappings.

## Enabled Writes

The user-facing connector is read-only, though `CREATE` and `INSERT` are written to simplify testing.
The source code generates an RSD schema file (the user-facing documentation has more details on schema files).
It could be user-facing if desired, but it was not in scope at this time for the first iteration.

Additionally, the CData driver does not support creating on-demand DynamoDB tables.
You must provide a numeric value for the provisioned read/write throughput.
If we wanted to support on-demand tables, we would need to create the tables
using the AWS SDK rather than using the CData stored procedure.

## Predicate Pushdown

The CData driver returns predicates where you compare to null as `true`, e.g. `value != 123` is true when `value` is `null`.
Trino treats this as false and causes correctness issues when these predicates are pushed down to the driver.
I started a support thread with CData and this is the expected behavior of the driver.
All pushdowns are disabled in the `DynamoDbJdbcClient` column mappings.

## Testing the Connector on AWS

Until product tests are in place, this connector can be tested locally or on EC2 by setting up IAM roles and policies by hand.
It assumes whoever is reading this has understanding of how to set up and configure SEP as well as creating IAM users and roles and setting policies using the AWS console.
This section can also be used as a guide for how to set up AWS for product tests.

To fully test the AWS authentication, you should use EC2 since we also support using the EC2 instance role for interacting with DynamoDB.

1. Install SEP as a single-node or multi-node cluster. It is expected to be launch-ready and just needs the Dynamo configs.
2. Create a new user in [IAM](https://console.aws.amazon.com/iamv2/home#/users) called, e.g., `sep-dynamodb` that needs an Access key.
   Copy the access key, secret key, and the ARN and keep them in a safe place. We'll use them later.
3. Attach, at a minimum, the `AmazonDynamoDBFullAccess` policy.
4. Create a `sep-dynamodb` profile in your `~/.aws/credentials` file containing the access and secret key.
5. Create a Dynamo DB table either via the console or using the AWS CLI and add a couple items:

```
aws dynamodb create-table \
    --table-name MusicCollection \
    --attribute-definitions AttributeName=Artist,AttributeType=S AttributeName=SongTitle,AttributeType=S \
    --key-schema AttributeName=Artist,KeyType=HASH AttributeName=SongTitle,KeyType=RANGE \
    --provisioned-throughput ReadCapacityUnits=5,WriteCapacityUnits=5 \
    --profile sep-dynamodb \
    --region us-east-2

aws dynamodb put-item \
    --table-name MusicCollection  \
    --item \
        '{"Artist": {"S": "No One You Know"}, "SongTitle": {"S": "Call Me Today"}, "AlbumTitle": {"S": "Somewhat Famous"}, "Awards": {"N": "1"}}' \
    --profile sep-dynamodb \
    --region us-east-2

aws dynamodb put-item \
    --table-name MusicCollection \
    --item \
        '{"Artist": {"S": "Acme Band"}, "SongTitle": {"S": "Happy Day"}, "AlbumTitle": {"S": "Songs About Life"}, "Awards": {"N": "10"} }' \
    --profile sep-dynamodb \
    --region us-east-2

aws dynamodb scan \
    --table-name MusicCollection \
    --profile sep-dynamodb \
    --region us-east-2
```

6. We'll start by testing using a user's access key and secret key.
Create a catalog file `sep-dynamodb.properties` in the `etc/catalog/` folder on each SEP node, adding the access and secret keys for the user.

```
connector.name=dynamodb
dynamodb.aws-access-key=<access key>
dynamodb.aws-secret-key=<secret key>
dynamodb.aws-region=us-east-2
dynamodb.generate-schema-files=ON_USE
dynamodb.schema-directory=etc/dynamodb-schemas
```

7. Create the folder `etc/dynamodb-schemas` on every host an start SEP.

8. Query the `musiccollection` table to ensure connectivity:
```
$ trino --server localhost:8080 --catalog sep-dynamodb --schema amazondynamodb
trino:amazondynamodb> show tables;
      Table
-----------------
 musiccollection
(1 row)

trino:amazondynamodb> select * from musiccollection;
   songtitle   |     artist      |    albumtitle
---------------+-----------------+------------------
 Call Me Today | No One You Know | Somewhat Famous
 Happy Day     | Acme Band       | Songs About Life
(2 rows)
```

9. Next we'll test the assume role functionality. Go to IAM and create a new role for another AWS account and enter the same account ID (or another account ID if you want to test cross-account access).
Optionally select an external ID to be added to the catalog file.
This role should have the `AmazonDynamoDBReadOnlyAccess` policy. Give it a name, e.g. `sep-dynamodb-read-only`. Hit create, pick the new role, and copy the ARN.
Under `Trust relationships` for the role, edit the trust relationship to include the ARN for the **user** from step 2.

11. Stop SEP and edit the catalog file to add this additional property:

```
dynamodb.aws-role-arn=<copied ARN for the role>

# If you also added an external id:
dynamodb.aws-external-id=<thevalue>
```

12. Query again and validate data can be read.

13. If running on EC2, you can remove the access key and secret key properties from the catalog file and the EC2 instance role attached to the instance will be used instead.
Make sure the instance role has the `AmazonDynamoDBReadOnlyAccess` policy attached to it in order to read from DynamoDB. The EC2 instance role would also need to be added to the
trust relationships for the role ARN if configured in the catalog.
