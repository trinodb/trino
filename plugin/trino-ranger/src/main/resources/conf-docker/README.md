# Apache Ranger authorizer for Trino
* Configuration files in this directory can be used to setup Trino running in a docker container to use Apache Ranger as the authorizer
* The authorizer will enforce policies defined in Apache Ranger running in a docker container, listening at http://host.docker.internal:6080
* The authorizer will write access audits in Apache Solr running in a docker container, listening at http://host.docker.internal:8983

## Requirements
* Following docker containers running in the same host:
  * Trino
  * Apache Ranger (details in https://github.com/apache/ranger/blob/master/dev-support/ranger-docker/README.md)
  * Apache Solr (details in https://github.com/apache/ranger/blob/master/dev-support/ranger-docker/README.md)
* Apache Ranger should have a service instance named dev_trino, containing policies to be enforced

## Configuration
* Copy following configuration files into Trino docker container under directory /etc/trino:
  * access-control.properties
  * ranger-trino-security.xml
  * ranger-trino-audit.xml
  * ranger-policymgr-ssl.xml
* Execute following commands inside Trino docker container to create a symbolic link:
```
cd /usr/lib/trino/plugin/ranger
ln -s /etc/trino conf
```
* Restart Trino container

## Verify
* Verify that Apache Ranger authorizer downloaded policies from the server
  * Wait for Trino to complete restart after above configuration
  * Login to Apache Ranger admin console
  * From left navigation bar, click on Audits/Plugin Status
  * In the search bar inside Plugin Status tab, set Service Type filter to Trino
  * Trino plugin should be listed, along with its IP address, hostname, policy download time, etc
  * If no Trino plugin is listed, review  Trino server logs (in the console) for any errors

* Verify that Apache Ranger authorizer writes access audit records in Apache Solr
  * Using Trino console, perform some operations - like create table, select from a table, drop table
  * From left navigation bar in Ranger admin console, click on Audits/Access
  * In the search bar inside Access tab, set Service Type filter to Trino
  * Audit logs for above operations should be listed with the following details:
    * name of the user who performed the operations
    * time when the operation was performed
    * name of the resources (catalog/schema/table/column) accessed
    * name of the operation performed (CreateTable, AccessCatalog, ShowSchemas, ShowTables, SelectFromColumn)
    * link to the Apache Ranger policy that allowed/denied the operation
