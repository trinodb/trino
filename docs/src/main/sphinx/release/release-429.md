# Release 429 (11 Oct 2023)

## General

* Allow {doc}`/sql/show-functions` for a specific schema. ({issue}`19243`)
* Add security for function listing. ({issue}`19243`)

## Security

* Stop performing security checks for functions in the `system.builtin` schema. ({issue}`19160`)
* Remove support for using function kind as a rule in file-based access control. ({issue}`19160`)

## Web UI

* Log out from a Trino OAuth session when logging out from the Web UI. ({issue}`13060`)

## Delta Lake connector

* Allow using the `#` and `?` characters in S3 location paths or URLs. ({issue}`19296`)

## Hive connector

* Add support for changing a column's type from `varchar` to `date`. ({issue}`19201`)
* Add support for changing a column's type from `decimal` to `tinyint`,
  `smallint`, `integer`, or `bigint` in partitioned Hive tables. ({issue}`19201`)
* Improve performance of reading ORC files. ({issue}`19295`)
* Allow using the `#` and `?` characters in S3 location paths or URLs. ({issue}`19296`)
* Fix error reading Avro files when a schema has uppercase characters in its
  name. ({issue}`19249`)

## Hudi connector

* Allow using the `#` and `?` characters in S3 location paths or URLs. ({issue}`19296`)

## Iceberg connector

* Add support for specifying timestamp precision as part of
  `CREATE TABLE AS .. SELECT` statements. ({issue}`13981`)
* Improve performance of reading ORC files. ({issue}`19295`)
* Allow using the `#` and `?` characters in S3 location paths or URLs. ({issue}`19296`)

## MongoDB connector

* Fix mixed case schema names being inaccessible when using custom roles and
  the `case-insensitive-name-matching` configuration property is enabled. ({issue}`19218`)

## SPI

* Change function security checks to return a boolean instead of throwing an
  exception. ({issue}`19160`)
* Add SQL path field to `ConnectorViewDefinition`,
  `ConnectorMaterializedViewDefinition`, and `ViewExpression`. ({issue}`19160`)
