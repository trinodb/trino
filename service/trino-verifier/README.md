# Trino Verifier

The Trino Verifier can be used to test Trino against a database, such
as MySQL, or to test two Trino clusters against each other. We use it
to continuously test the master branch against the previous release,
while developing Trino.

To use the Verifier, create a MySQL database with the following table
and load it with the queries you would like to run:

```sql
CREATE TABLE verifier_queries(
    id INT NOT NULL AUTO_INCREMENT,
    suite VARCHAR(256) NOT NULL,
    name VARCHAR(256),
    test_catalog VARCHAR(256) NOT NULL,
    test_schema VARCHAR(256) NOT NULL,
    test_prequeries MEDIUMTEXT,
    test_query MEDIUMTEXT NOT NULL,
    test_postqueries MEDIUMTEXT,
    test_username VARCHAR(256) NOT NULL default 'verifier-test',
    test_password VARCHAR(256),
    test_session_properties_json VARCHAR(2048),
    control_catalog VARCHAR(256) NOT NULL,
    control_schema VARCHAR(256) NOT NULL,
    control_prequeries MEDIUMTEXT,
    control_query MEDIUMTEXT NOT NULL,
    control_postqueries MEDIUMTEXT,
    control_username VARCHAR(256) NOT NULL default 'verifier-test',
    control_password VARCHAR(256),
    control_session_properties_json VARCHAR(2048),
    PRIMARY KEY (id)
);
```

Next, create a properties file to configure the verifier:

```
suite=my_suite
query-database=jdbc:mysql://localhost:3306/my_database?user=my_username&password=my_password
control.gateway=jdbc:trino://localhost:8080
test.gateway=jdbc:trino://localhost:8081
thread-count=1
```

Lastly, download the [Maven verifier plugin][maven_download] for the same 
release as your Trino instance by navigating to the directory for that 
release, and selecting the ``trino-verifier-*.jar`` file. Once it is downloaded,
rename it to `verifier`, make it executable with `chmod +x`, then run it:

[maven_download]: https://repo.maven.apache.org/maven2/io/trino/trino-verifier/

```
./verifier config.properties
```
