# Apache Ranger authorizer for Trino
* This plugin supports use of Apache Ranger policies to authorize data access in Trino - like operations on catalogs, schemas, tables, columns.
* Column-masking and row-filtering are supported in this plugin.
* Accesses authorized by the plugin are audited for compliance purposes.

## Requirements
* Access to an Apache Ranger instance having authorization policies to be enforced by this plugin
* Access to audit stores (Solr/Elasticsearch/S3/HDFS) to save access audit logs

## Configuration
Add following entries in /etc/trino/access-control.properties to configure Apache Ranger as the authorizer in Trino:

access-control.name ranger

ranger.service_name          dev_trino
ranger.security_config       /etc/trino/ranger-trino-security.xml
ranger.audit_config          /etc/trino/ranger-trino-audit.xml
ranger.policy_mgr_ssl_config /etc/trino/ranger-trino-policymgr-ssl.xml

Apache Ranger plugin configurations for policy store and audit store should be updated in following configuration file:
/etc/trino/ranger-trino-security.xml
/etc/trino/ranger-trino-audit.xml
/etc/trino/ranger-trino-policymgr-ssl.xml```
