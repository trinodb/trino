# Release 435 (13 Dec 2023)

## General

* Add support for the `json_table` table function. ({issue}`18017`)
* Reduce coordinator memory usage. ({issue}`20018`, {issue}`20022`)
* Increase reliability and memory consumption of inserts. ({issue}`20040`)
* Fix incorrect results for `LIKE` with some strings containing repeated
  substrings. ({issue}`20089`)
* Fix coordinator memory leak. ({issue}`20023`)
* Fix possible query failure for `MERGE` queries when `retry-policy` set to
  `TASK` and `query.determine-partition-count-for-write-enabled` set to `true`.
  ({issue}`19979`)
* Prevent hanging query processing with `retry.policy` set to `TASK` when a
  worker node died. ({issue}`18603 `)
* Fix query failure when reading array columns. ({issue}`20065`)

## Delta Lake connector

* {{breaking}} Remove support for registering external tables with
  `CREATE TABLE` and the `location` table property. Use the
  `register_table` procedure as replacement. The property
  `delta.legacy-create-table-with-existing-location.enabled` is
  also removed. ({issue}`17016`)
* Improve query planning performance on Delta Lake tables. ({issue}`19795`)
* Ensure AWS access keys are used for connections to the AWS Security Token
  Service. ({issue}`19982`)
* Reduce memory usage for inserts into partitioned tables. ({issue}`19649`)
* Improve reliability when reading from GCS. ({issue}`20003`)
* Fix failure when reading ORC data. ({issue}`19935`)

## Elasticsearch connector

* Ensure certificate validation is skipped when
  `elasticsearch.tls.verify-hostnames` is `false`. ({issue}`20076`)

## Hive connector

* Add support for columns that changed from integer types to `decimal` type. ({issue}`19931`)
* Add support for columns that changed from `date` to `varchar` type. ({issue}`19500`)
* Rename `presto_version` table property to `trino_version`. ({issue}`19967`)
* Rename `presto_query_id` table property to `trino_query_id`. ({issue}`19967`)
* Ensure AWS access keys are used for connections to the AWS Security Token
  Service. ({issue}`19982`)
* Improve query planning time on Hive tables without statistics. ({issue}`20034`)
* Reduce memory usage for inserts into partitioned tables. ({issue}`19649`)
* Improve reliability when reading from GCS. ({issue}`20003`)
* Fix failure when reading ORC data. ({issue}`19935`)

## Hudi connector

* Ensure AWS access keys are used for connections to the AWS Security Token
  Service. ({issue}`19982`)
* Improve reliability when reading from GCS. ({issue}`20003`)
* Fix failure when reading ORC data. ({issue}`19935`)

## Iceberg connector

* Fix incorrect removal of statistics files when executing
  `remove_orphan_files`. ({issue}`19965`)
* Ensure AWS access keys are used for connections to the AWS Security Token
  Service. ({issue}`19982`)
* Improve performance of metadata queries involving materialized views. ({issue}`19939`)
* Reduce memory usage for inserts into partitioned tables. ({issue}`19649`)
* Improve reliability when reading from GCS. ({issue}`20003`)
* Fix failure when reading ORC data. ({issue}`19935`)

## Ignite connector

* Improve performance of queries involving `OR` with `IS NULL`, `IS NOT NULL`
  predicates, or involving `NOT` expression by pushing predicate computation to
  the Ignite database. ({issue}`19453`)

## MongoDB connector

* Allow configuration to use local scheduling of MongoDB splits with
  `mongodb.allow-local-scheduling`. ({issue}`20078`)

## SQL Server connector

* Fix incorrect results when reading dates between `1582-10-05` and
  `1582-10-14`. ({issue}`20005`)
