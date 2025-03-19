# Release 473 (19 Mar 2025)

## General

* Add support for array literals. ({issue}`25301`)
* Reduce the amount of memory required for `DISTINCT` and `GROUP BY` operations. ({issue}`25127`)
* Improve performance of `GROUP BY` and `DISTINCT` aggregations when spilling to disk is enabled  
  or grouping by `row`, `array`, or `map` columns ({issue}`25294`)
* Fix failure when setting comments on columns with upper case letters. ({issue}`25297`)
* Fix potential query failure when `retry_policy` set to `TASK` ({issue}`25217`)

## Security

* Add LDAP-based group provider. ({issue}`23900`)
* Fix column masks not being applied on view columns with upper case. ({issue}`24054`)

## BigQuery connector

* Fix failure when initializing the connector on a machine with more than 32 CPU cores. ({issue}`25228`)

## Delta Lake connector

* Remove the deprecated `glue-v1` metastore type. ({issue}`25201`)
* Remove deprecated Databricks Unity catalog integration. ({issue}`25250`)
* Fix Glue endpoint URL override. ({issue}`25324`)

## Hive connector

* Remove the deprecated `glue-v1` metastore type. ({issue}`25201`)
* Remove deprecated Databricks Unity catalog integration. ({issue}`25250`)
* Fix Glue endpoint URL override. ({issue}`25324`)

## Hudi connector

* Fix queries getting stuck when reading empty partitions. ({issue}`19506 `)
* Remove the deprecated `glue-v1` metastore type. ({issue}`25201`)
* Fix Glue endpoint URL override. ({issue}`25324`)

## Iceberg connector

* Set the `write.<filetype>.compression-codec` table property when creating new tables. ({issue}`24851`)
* Expose additional properties in `$properties` tables. ({issue}`24812`)
* Fix Glue endpoint URL override. ({issue}`25324`)

## Kudu connector

* Remove the Kudu connector. ({issue}`24417`)

## Phoenix connector

* Remove the Phoenix connector. ({issue}`24135`)

## SPI

* Add `SourcePage` interface and `ConnectorPageSource.getNextSourcePage()`. ({issue}`24011`)
* Deprecate `ConnectorPageSource.getNextPage()` for removal. ({issue}`24011`)
