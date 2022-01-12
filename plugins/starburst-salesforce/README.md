# Starburst Enterprise Salesforce Connector

Rather than using Salesforce's native APIs and having a lot of code to write and maintain,
we instead leverage CData's Salesforce JDBC Driver to abstract away the complexity of communicating with Salesforce.
This leads to easier integration, however there are some quirks with the driver that developers should be aware of which is detailed below. 

## About the CData JDBC Driver

### Overview

The full documentation for the CData JDBC Driver for Salesforce can be found at [CData's website here](http://cdn.cdata.com/help/RFF/jdbc/default.htm).

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
The OEM key is hardcoded in `SalesforceConnectionFactory` in case it ever needs updated.

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
-Dfile=/path/to/cdata-salesforce-jdbc.jar \
-DgroupId=com.cdata \
-DartifactId=salesforce-jdbc \
-Dversion=2021-2 \
-Dpackaging=jar \
-DrepositoryId=starburstdata.releases \
-Durl=https://maven.starburstdata.net/starburstdata-artifacts/releases
```

## Custom Tables and Columns in Salesforce

Salesforce identifies any custom objects a user creates by adding `__c` to the object name.
This means when we run a `CREATE TABLE` command, the table and column names will all end in `__c`.
This complicates testing since it is expected that you're able to insert and select data using the same names when you made the table.
Because of this, the `TestSalesforceConnectorTest` contains a copy of all tests from the `BaseJdbcConnectorTest` and parent
classes with the queries changed to use table and column names ending in `__c`.
Additionally, each table has some Salesforce system columns that were not declared in the DDL but will show up when the table is described.

You'll also notice some classes that were copied from `trino-testing` to support the `TestSalesforceTypeMapping`.
They were copied because 1) the table and column names are different and 2) CData caches metadata which is documented below.

## CData Metadata Cache

CData drivers automatically cache metadata about tables and columns from Salesforce.
This metadata is not refreshed whenever a new table is created, so inserting data after creating a table may fail.
You can reset the metadata cache by executing `RESET SCHEMA CACHE` on the connection.
CData keeps a cache per distinct set of JDBC connection properties, so if you are using a separate connection
you will need to make sure you provide the same set of properties.
The SalesforceJdbcClient will invalidate the cache after a table is created.

In practice, I am not sure how much users will hit this metadata caching issue.
The connector is read-only, so they won't be able to create tables and insert data using Trino.
They'd have to open a Trino connection, run a query against Salesforce to cache metadata, create the table in Salesforce and upload data to it, and then go and query it again.


## Data Type Mappings

[CData documentation](http://cdn.cdata.com/help/RFF/jdbc/pg_datatypemapping.htm) for type mappings.

`TestSalesforceTypeMapping` uses only static table names rather than a table with a random suffix.
Tables created by the driver are "custom objects", and Salesforce has a limit on the number of custom objects you can have.
When a table is dropped, the custom object is soft-deleted and goes into a bin where it is eventually
hard deleted after a couple weeks. Because of this, we can quickly reach the limit on the number of custom
objects due to CI builds. There is no way to programatically hard delete a table, so instead
we create static table names and truncate them rather than deleting them. The Salesforce CI job is configured
to only run one job at a time across all builds.

Additionally, the TPC-H tables are also only created and the data is only loaded if they do not exist.
This is to cut down on the custom object limit as well as to cut down on CI build times as
loading the TPC-H data into Salesforce can take several minutes.

Note that these custom objects can be deleted by hand in Salesforce using the Object Manager.

### Booleans

Salesforce has a "Checkbox" data type which mato a CData `bool`.
In practice, if you create a table with a `boolean` type the driver wants a max length, and then it turns into a `varchar` on read.
You then pass strings to the drivers for the values, e.g. `0` or `1` for `boolean(1)` and `true` or `false` for `boolean(5)`.

For example, you can create a table like below, however describing the table results in the columns `a` and `b` to be `varchar(1)` and `varchar(5)` respectively.

```sql
CREATE TABLE test_booleans (a boolean(1), b boolean(5))
```

For this reason Booleans are untested but supported in the column mappings.

### Numeric types

All numeric types from the CData driver come back as a `double` type.
We have column mappings for integer, bigint, and decimal however they are not tested at this time as the data type metadata come back as `double` on read. 

### Timestamp/Time Precision

Salesforce documents state millisecond precision, however the CData driver truncates all precision timestamp and time types with no rounding.
This is for both reads and writes to Salesforce.

### Varchar

Salesforce has a max length of 255 characters for `varchar` types and requires the length to be specified


## CI Tests - Password Expiration

**Note:** The tests now use OAuth authentication instead of basic user/password. This section is no longer relevant for the tests, but the user's password will
still expire and anyone logging in would be prompted to change the password.

The CI tests are configured to run with two tests users, including their passwords and security tokens stored as GitHub secrets and referenced in the `ci.yml` file.
Periodically, the passwords will expire and the tests will fail with an `INVALID_OPERATION_WITH_EXPIRED_PASSWORD` error.
A new password will need to be created, which generates a new security token as well.
Currently, only the Test User 1 account is used, but there is also a sep.salesforcedl.test2@starburstdata.com user in case it ends up being used in the future.

1. Login to https://starburstdata--partial.my.salesforce.com/ as sep.salesforcedl.test1@starburstdata.com and the current (expired) password
2. You will be prompted to create a new password. Once created, an email is sent to sep.salesforcedl.test1@starburstdata.com containing the new security token
    * wbiela is a member of the DL
3. Update the GitHub secrets for `SALESFORCE_USER1_PASSWORD` and `SALESFORCE_USER1_SECURITY_TOKEN` to the new values
4. Let the CI build run; the tests should no longer fail with an expired password error

## CI Tests - OAuth

The connector now supports OAuth using JWT in order to not require a password change. The CData driver supports a few types of OAuth, however the JWT
method is the only one that doesn't require a user to login to Salesforce. The trust is set up on the Salesforce side using a certificate and some configuration.
The following steps document how to set up OAuth JWT for the test user in case they are needed down the line (like the app in Salesforce is accidentally deleted).
At a high-level, you create a PKCS12 and certificate then create an OAuth-enabled Connected App in Salesforce that uses that certificate. You then give permission to the
test user to use that Connected App and configure the Salesforce connector to use the PKCS12 and user name. This sets up a trust that will never expire until it is explicitly
revoked by a user in the Salesforce UI.

1. Create a password and export it as the `PKCS12_PASS` environment variable. Keep this password as it will be needed by the tests to unlock the PKCS12 (stored as a GitHub secret)

2. Create a PKCS12

```bash
keytool -genkeypair -v \
  -alias salesforce-ca \
  -dname "CN=StarburstSalesforce, OU=Starburst Connectors Team, O=Starburst Data, L=Boston, ST=MA, C=US" \
  -storetype PKCS12 \
  -keystore salesforce-ca.p12 \
  -keypass:env PKCS12_PASS \
  -storepass:env PKCS12_PASS \
  -keyalg RSA \
  -keysize 4096 \
  -ext KeyUsage:critical="keyCertSign" \
  -ext BasicConstraints:critical="ca:true" \
  -validity 9999
```

3. Export the certificate

```bash
keytool -export -v \
  -alias salesforce-ca \
  -file salesforce-ca.crt \
  -keypass:env PKCS12_PASS \
  -storepass:env PKCS12_PASS \
  -keystore salesforce-ca.p12 \
  -rfc
```

4. Create and configure the Connected App

Login to [Starburst's Salesforce Sandbox](https://starburstdata--partial.my.salesforce.com/) using `sep.salesforcedl.test1@starburstdata.com` and the password.
Select the gear icon in the upper right and select `Setup`, then `Apps -> App Manager` and select `New Connected App` in the upper right.
Enter a name for the Connected App, e.g. `SEP Salesforce Connector CI` and a contact email, e.g. `sep.salesforcedl.test1@starburstdata.com`.
A Callback URL is required but unused by this method, enter a valid URL, e.g. `http://localhost`.
Select `Enable OAuth Settings` and `Use digital signatures`, uploading `salesforce-ca.crt` to the site.
For `Selected OAuth Scopes` use `Access content resources (content)` at a minimum.
Select `Save` and `Continue`.
Copy and keep the `Consumer Key` which is needed to configure the connector.

5. Create a new Permission Set.
Navigate to `Users -> Permissions Sets` and select `New`.
Enter a label, e.g. `SEP Salesforce Connector CI Permission Set` and select `Save`.
Select `Assigned Connected Apps` then `Edit`, then add the connected app name created in the previous step.

6. Add the Permission Set to the User

Select `Users` and then find the user you want to use to authenticate with Salesforce, clicking on the name.
Select `Permission Set Assignments` then `Edit Assignments` and add the permission set to the user and save.
This concludes the Salesforce setup.

7. Configure the Salesforce catalog to enable OAuth JWT.  You'll need the PKCS12 password, the Consumer Key from Salesforce, and the user's email address login.

```properties
salesforce.authentication.type=OAUTH_JWT
salesforce.oauth.jks-path=/path/to/salesforce-ca.jks
salesforce.oauth.jks-password=${ENV:PKCS12_PASS}
salesforce.oauth.jwt-issuer=<consumer key from Salesforce connected app>
salesforce.oauth.jwt-subject=<user name or email address, e.g. sep.salesforcedl.test1@starburstdata.com
```