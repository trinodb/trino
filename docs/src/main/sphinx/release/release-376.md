# Release 376 (7 Apr 2022)

## General

* Add table redirection awareness for `RENAME table` operations. ({issue}`11277`)
* Deny adding column with comment if the connector does not support this feature. ({issue}`11486`)
* Improve performance for queries that contain inequality expressions. ({issue}`11518`)
* Consider null values as identical values in `array_except`, `array_union`,
  `map_concat`, `map_from_entries`, `multimap_from_entries`, and `multimap_agg`
  functions. ({issue}`560`)
* Fix failure of `DISTINCT .. LIMIT` operator when input data is dictionary
  encoded. ({issue}`11776`)
* Fix returning of invalid results for distinct aggregation when input data is
  dictionary encoded. ({issue}`11776`)
* Fix query failure when performing joins with connectors that support index lookups. ({issue}`11758`)
* Fix incorrect stage memory statistics reporting for queries running with
  `retry-policy` set to `TASK`. ({issue}`11801`)

## Security

* Add support to use two-way TLS/SSL certificate validation with LDAP authentication. ({issue}`11070`)
* Fix failures in information schema role tables for catalogs using system roles. ({issue}`11694`)

## Web UI

* Add new page to display the runtime information of all workers in the cluster. ({issue}`11653`)

## JDBC driver

* Add support for using the system truststore with the `SSLUseSystemTrustStore` parameter. ({issue}`10482`)
* Add support for `ResultSet.getAsciiStream()` and `ResultSet.getBinaryStream()`. ({issue}`11753`)
* Remove `user` property requirement. ({issue}`11350`)

## CLI

* Add support for using the system truststore with the `--use-system-truststore` option. ({issue}`10482`)

## Accumulo connector

* Add support for adding and dropping schemas. ({issue}`11808`)
* Disallow creating tables in a schema that doesn't exist. ({issue}`11808`)

## ClickHouse connector

* Add support for column comments when creating new tables. ({issue}`11606`)
* Add support for column comments when adding new columns. ({issue}`11606`)

## Delta Lake connector

* Add support for `INSERT`, `UPDATE`, and `DELETE` queries on Delta Lake tables
  with fault-tolerant execution. ({issue}`11591`)
* Allow setting duration for completion of [dynamic filtering](/admin/dynamic-filtering)
  with the `delta.dynamic-filtering.wait-timeout` configuration property. ({issue}`11600`)
* Improve query planning time after `ALTER TABLE ... EXECUTE optimize` by always
  creating a transaction log checkpoint. ({issue}`11721`)
* Add support for reading Delta Lake tables in with auto-commit mode disabled. ({issue}`11792`)

## Hive connector

* Store file min/max ORC statistics for string columns even when actual min or
  max value exceeds 64 bytes. This improves query performance when filtering on
  such column. ({issue}`11652`)
* Improve performance when reading Parquet data. ({issue}`11675`)

## Iceberg connector

* Add support for views when using Iceberg Glue catalog. ({issue}`11499`)
* Add support for reading Iceberg v2 tables containing deletion files. ({issue}`11642`)
* Add support for table redirections to the Hive connector. ({issue}`11356`)
* Include non-Iceberg tables when listing tables from Hive catalogs. ({issue}`11617`)
* Expose `nan_count` in the `$partitions` metadata table. ({issue}`10709`)
* Store file min/max ORC statistics for string columns even when actual min or
  max value exceeds 64 bytes. This improves query performance when filtering on
  such column. ({issue}`11652`)
* Improve performance when reading Parquet data. ({issue}`11675`)
* Improve query performance when the same table is referenced multiple times
  within a query. ({issue}`11650`)
* Fix NPE when an Iceberg data file is missing null count statistics. ({issue}`11832`)

## Kudu connector

* Add support for adding columns with comment. ({issue}`11486`)

## MySQL connector

* Improve performance of queries involving joins by pushing computation to the
  MySQL database. ({issue}`11638`)

## Oracle connector

* Improve query performance of queries involving aggregation by pushing
  aggregation computation to the Oracle database. ({issue}`11657`)

## SPI

* Add support for table procedures that execute on the coordinator only. ({issue}`11750`)
