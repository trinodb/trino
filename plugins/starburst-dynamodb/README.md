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
mvn deploy:deploy-file \
-Dfile=/path/to/cdata-dynamodb-jdbc.jar \
-DgroupId=com.cdata \
-DartifactId=dynamodb-jdbc \
-Dversion=2020-4 \
-Dpackaging=jar \
-DrepositoryId=starburstdata.releases \
-Durl=https://maven.starburstdata.net/starburstdata-artifacts/releases
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