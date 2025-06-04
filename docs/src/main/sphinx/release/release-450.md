# Release 450 (19 Jun 2024)

## General

* Add support for specifying an Azure blob endpoint for accessing spooling in
  fault-tolerant execution with the `exchange.azure.endpoint` configuration
  property. ({issue}`22218`)
* Expose driver execution statistics via JMX. ({issue}`22427`)
* Improve performance of the {func}`first_value` and {func}`last_value`
  functions. ({issue}`22092`)
* Improve performance for large clusters under heavy workloads. ({issue}`22039`)
* Improve performance of queries with simple predicates. This optimization can
  be disabled using the `experimental.columnar-filter-evaluation.enabled`
  configuration property or the `columnar_filter_evaluation_enabled` session
  property. ({issue}`21375`)
* {{breaking}} Improve performance of aggregations containing a `DISTINCT`
  clause, and replace the `optimizer.mark-distinct-strategy` and
  `optimizer.optimize-mixed-distinct-aggregations` configuration properties with
  the new `optimizer.distinct-aggregations-strategy` property. ({issue}`21907`)
* Improve performance of reading JSON files. ({issue}`22348`)
* Improve performance for the {func}`date_trunc`, {func}`date_add`, and
  {func}`date_diff` functions. ({issue}`22192`)
* Fix failure when loading the [](/admin/event-listeners-openlineage). ({issue}`22228`)
* Fix potential incorrect results when metadata or table data in certain
  connectors is updated or deleted. ({issue}`22285`)

## Security

* Add support for using web identity exclusively for authentication when running
  on Amazon EKS with the legacy S3 file system enabled. This can be configured
  via the `trino.s3.use-web-identity-token-credentials-provider` property. ({issue}`22162`)
* Add support for exclusively using web identity for authentication when using
  Amazon EKS with
  [IAM roles](https://docs.aws.amazon.com/eks/latest/userguide/iam-roles-for-service-accounts.html)
  by setting the
  `s3.use-web-identity-token-credentials-provider` configuration property. ({issue}`22163`)

## JDBC driver

* Add support for the `assumeNullCatalogMeansCurrent` connection property. When
  enabled, a `null` value for the `catalog` parameter in `DatabaseMetaData`
  methods is assumed to mean the current catalog. If no current catalog is
  set, the behaviour is unmodified. ({issue}`20866`)

## BigQuery connector

* Add support for metadata caching when the
  `bigquery.case-insensitive-name-matching` configuration property is enabled. ({issue}`10740`)
* {{breaking}} Automatically configure BigQuery scan parallelism, and remove the
  `bigquery.parallelism` configuration property. ({issue}`22279`)

## Cassandra connector

* Fix incorrect results when specifying a value for the
  `cassandra.partition-size-for-batch-select` configuration property. ({issue}`21940`)

## ClickHouse connector

* Improve performance of `ORDER BY ... LIMIT` on non-textual types by pushing
  execution down to the underlying database. ({issue}`22174`)

## Delta Lake connector

* Add support for concurrent `UPDATE`, `MERGE`, and `DELETE` queries. ({issue}`21727`)
* Add support for using table statistics with `TIMESTAMP` types. ({issue}`21878`)
* Add support for reading tables with
  [type widening](https://docs.delta.io/latest/delta-type-widening.html). ({issue}`21756`)
* Set the default value for the `s3.max-connections` configuration property
  to 500. ({issue}`22209`)
* Fix failure when reading a `TIMESTAMP` value after the year 9999. ({issue}`22184`)
* Fix failure when reading tables with the unsupported `variant` type. ({issue}`22310`)
* Add support for reading
  [UniForm](https://docs.delta.io/latest/delta-uniform.html) tables. ({issue}`22106`)

## Hive connector

* Add support for changing a column's type from `integer` to `varchar` and
  `decimal` to `varchar`, respectively, in unpartitioned tables. ({issue}`22246`, {issue}`22293`)
* Add support for changing a column's type from `double` to `varchar` in
  unpartitioned tables
  using Parquet files. ({issue}`22277`)
* Add support for changing a column's type from `float` to `varchar`. ({issue}`22291`)
* Set the default value for the `s3.max-connections` configuration property
  to 500. ({issue}`22209`)

## Hudi connector

* Set the default value for the `s3.max-connections` configuration property
  to 500. ({issue}`22209`)

## Iceberg connector

* Add support for the `TRUNCATE` statement. ({issue}`22340`)
* {{breaking}} Add support for V2 of the Nessie REST API. Previous behavior can
  be restored by setting the `iceberg.nessie-catalog.client-api-version`
  configuration property to `V1`. ({issue}`22215`)
* Improve performance when reading by populating `split_offsets` in file
  metadata. ({issue}`9018`)
* Set the default value for the `s3.max-connections` configuration property
  to 500. ({issue}`22209`)
* Fix failure when reading Parquet files that don't have `field-id` on
  structured types. ({issue}`22347`)

## MariaDB connector

* Add support for [fault-tolerant execution](/admin/fault-tolerant-execution). ({issue}`22328`)
* Improve performance of listing table columns. ({issue}`22241`)

## Memory connector

* Add support for the `TRUNCATE` statement. ({issue}`22337`)

## MySQL connector

* Improve performance of listing table columns. ({issue}`22241`)

## Pinot connector

* Add support for the
  [`enableNullHandling` query option](https://docs.pinot.apache.org/developers/advanced/null-value-support#advanced-null-handling-support). ({issue}`22214`)
* Fix failure when using [dynamic tables](pinot-dynamic-tables). ({issue}`22301`)

## Redshift connector

* Improve performance of listing table columns. ({issue}`22241`)

## SingleStore connector

* Improve performance of listing table columns. ({issue}`22241`)
