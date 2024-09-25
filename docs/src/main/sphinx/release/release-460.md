# Release 460 (3 Oct 2024)

## General

* Fix failure for certain queries involving lambda expressions. ({issue}`23649`)

## Atop connector

* {{breaking}} Remove the Atop connector. ({issue}`23550`)

## ClickHouse connector

* Improve performance of listing columns. ({issue}`23429`)
* Improve performance for queries comparing `varchar` columns. ({issue}`23558`) 
* Improve performance for queries using `varchar` columns for `IN` comparisons. ({issue}`23581`)
* Improve performance for queries with complex expressions involving `LIKE`. ({issue}`23591`)

## Delta Lake connector

* Add support for using an [Alluxio cluster as file system
  cache](/object-storage/file-system-alluxio). ({issue}`21603`)
* Add support for WASBS to [](/object-storage/file-system-azure). ({issue}`23548`)
* Disallow writing to tables that both change data feed and [deletion
  vectors](https://docs.delta.io/latest/delta-deletion-vectors.html) are
  enabled. ({issue}`23653`)
* Fix query failures when writing bloom filters in Parquet files. ({issue}`22701`)

## Hive connector

* Add support for using an [Alluxio cluster as file system
  cache](/object-storage/file-system-alluxio). ({issue}`21603`)
* Add support for WASBS to [](/object-storage/file-system-azure). ({issue}`23548`)
* Fix query failures when writing bloom filters in Parquet files. ({issue}`22701`)

## Hudi connector

* Add support for WASBS to [](/object-storage/file-system-azure). ({issue}`23548`)

## Iceberg connector

* Add support for using an [Alluxio cluster as file system
  cache](/object-storage/file-system-alluxio). ({issue}`21603`)
* Add support for WASBS to [](/object-storage/file-system-azure). ({issue}`23548`)
* Ensure table columns are cached in Glue even when table comment is too long. ({issue}`23483`)
* Reduce planning time for queries on columns containing a large number of
  nested fields. ({issue}`23451`)
* Fix query failures when writing bloom filters in Parquet files. ({issue}`22701`)

## Oracle connector

* Improve performance for queries casting columns to `char` or to `varchar`. ({issue}`22728`)

## Raptor connector

* {{breaking}} Remove the Raptor connector. ({issue}`23588`)

## SQL Server connector

* Improve performance of listing columns. ({issue}`23429`)
