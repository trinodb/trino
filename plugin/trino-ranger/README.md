# Apache Ranger authorizer for Trino
This plugin enables Trino to use [Apache Ranger](https://ranger.apache.org/) policies to authorize access to Trino resources like catalogs, schemas, tables, columns. This plugin supports column-masking and row-filtering.

## Requirements
* Access to an Apache Ranger instance having authorization policies to be enforced by this plugin
* Access to audit stores (Solr/Elasticsearch/S3/HDFS) to save access audit logs

## Configuration
Add following entries in `/etc/trino/access-control.properties` to configure Apache Ranger as the authorizer in Trino:

````
access-control.name ranger

ranger.service_name          dev_trino
ranger.security_config       /etc/trino/ranger-trino-security.xml
ranger.audit_config          /etc/trino/ranger-trino-audit.xml
ranger.policy_mgr_ssl_config /etc/trino/ranger-trino-policymgr-ssl.xml
````

Apache Ranger plugin configurations for policy store and audit store should be updated in following configuration files:
````
/etc/trino/ranger-trino-security.xml
/etc/trino/ranger-trino-audit.xml
/etc/trino/ranger-trino-policymgr-ssl.xml
````
