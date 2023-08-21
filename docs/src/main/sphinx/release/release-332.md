# Release 332 (08 Apr 2020)

## General

- Fix query failure during planning phase for certain queries involving multiple joins. ({issue}`3149`)
- Fix execution failure for queries involving large `IN` predicates on decimal values with precision larger than 18. ({issue}`3191`)
- Fix prepared statements or view creation for queries containing certain nested aliases or `TABLESAMPLE` clauses. ({issue}`3250`)
- Fix rare query failure. ({issue}`2981`)
- Ignore trailing whitespace when loading configuration files such as
  `etc/event-listener.properties` or `etc/group-provider.properties`.
  Trailing whitespace in `etc/config.properties` and catalog properties
  files was already ignored. ({issue}`3231`)
- Reduce overhead for internal communication requests. ({issue}`3215`)
- Include filters over all table columns in output of `EXPLAIN (TYPE IO)`. ({issue}`2743`)
- Support configuring multiple event listeners. The properties files for all the event listeners
  can be specified using the `event-listener.config-files` configuration property. ({issue}`3128`)
- Add `CREATE SCHEMA ... AUTHORIZATION` syntax to create a schema with specified owner. ({issue}`3066`).
- Add `optimizer.push-partial-aggregation-through-join` configuration property to control
  pushing partial aggregations through inner joins. Previously, this was only available
  via the `push_partial_aggregation_through_join` session property. ({issue}`3205`)
- Rename configuration property `optimizer.push-aggregation-through-join`
  to `optimizer.push-aggregation-through-outer-join`. ({issue}`3205`)
- Add operator statistics for the number of splits processed with a dynamic filter applied. ({issue}`3217`)

## Security

- Fix LDAP authentication when user belongs to multiple groups. ({issue}`3206`)
- Verify access to table columns when running `SHOW STATS`. ({issue}`2665`)
- Only return views accessible to the user from `information_schema.views`. ({issue}`3290`)

## JDBC driver

- Add `clientInfo` property to set extra information about the client. ({issue}`3188`)
- Add `traceToken` property to set a trace token for correlating requests across systems. ({issue}`3188`)

## BigQuery connector

- Extract parent project ID from service account before looking at the environment. ({issue}`3131`)

## Elasticsearch connector

- Add support for `ip` type. ({issue}`3347`)
- Add support for `keyword` fields with numeric values. ({issue}`3381`)
- Remove unnecessary `elasticsearch.aws.use-instance-credentials` configuration property. ({issue}`3265`)

## Hive connector

- Fix failure reading certain Parquet files larger than 2GB. ({issue}`2730`)
- Improve performance when reading gzip-compressed Parquet data. ({issue}`3175`)
- Explicitly disallow reading from Delta Lake tables. Previously, reading
  from partitioned tables would return zero rows, and reading from
  unpartitioned tables would fail with a cryptic error. ({issue}`3366`)
- Add `hive.fs.new-directory-permissions` configuration property for setting the permissions of new directories
  created by Presto. Default value is `0777`, which corresponds to previous behavior. ({issue}`3126`)
- Add `hive.partition-use-column-names` configuration property and matching `partition_use_column_names` catalog
  session property that allows to match columns between table and partition schemas by names. By default they are mapped
  by index. ({issue}`2933`)
- Add support for `CREATE SCHEMA ... AUTHORIZATION` to create a schema with specified owner. ({issue}`3066`).
- Allow specifying the Glue metastore endpoint URL using the
  `hive.metastore.glue.endpoint-url` configuration property. ({issue}`3239`)
- Add experimental file system caching. This can be enabled with the `hive.cache.enabled` configuration property. ({issue}`2679`)
- Support reading files compressed with newer versions of LZO. ({issue}`3209`)
- Add support for Alluxio Catalog Service. ({issue}`2116`)
- Remove unnecessary `hive.metastore.glue.use-instance-credentials` configuration property. ({issue}`3265`)
- Remove unnecessary `hive.s3.use-instance-credentials` configuration property. ({issue}`3265`)
- Add flexible {ref}`hive-s3-security-mapping`, allowing for separate credentials
  or IAM roles for specific users or buckets/paths. ({issue}`3265`)
- Add support for specifying an External ID for an IAM role trust policy using
  the `hive.metastore.glue.external-id` configuration property ({issue}`3144`)
- Allow using configured S3 credentials with IAM role. Previously,
  the configured IAM role was silently ignored. ({issue}`3351`)

## Kudu connector

- Fix incorrect column mapping in Kudu connector. ({issue}`3170`, {issue}`2963`)
- Fix incorrect query result for certain queries involving `IS NULL` predicates with `OR`. ({issue}`3274`)

## Memory connector

- Include views in the list of tables returned to the JDBC driver. ({issue}`3208`)

## MongoDB connector

- Add `objectid_timestamp` for extracting the timestamp from `ObjectId`. ({issue}`3089`)
- Delete document from `_schema` collection when `DROP TABLE`
  is executed for a table that exists only in `_schema`. ({issue}`3234`)

## SQL Server connector

- Disallow renaming tables between schemas. Previously, such renames were allowed
  but the schema name was ignored when performing the rename. ({issue}`3284`)

## SPI

- Expose row filters and column masks in `QueryCompletedEvent`. ({issue}`3183`)
- Expose referenced functions and procedures in `QueryCompletedEvent`. ({issue}`3246`)
- Allow `Connector` to provide `EventListener` instances. ({issue}`3166`)
- Deprecate the `ConnectorPageSourceProvider.createPageSource()` variant without the
  `dynamicFilter` parameter. The method will be removed in a future release. ({issue}`3255`)
