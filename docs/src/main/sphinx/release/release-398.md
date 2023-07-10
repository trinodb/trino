# Release 398 (28 Sep 2022)

## General

* Add Hudi connector. ({issue}`10228`)
* Add metrics for the execution time of filters and projections to `EXPLAIN
  ANALYZE VERBOSE`. ({issue}`14135`)
* Show local cost estimates when using `EXPLAIN`. ({issue}`14268`)
* Fix timeouts happening too early because of improper handling of the
  `node-scheduler.allowed-no-matching-node-period` configuration property. ({issue}`14256`)
* Fix query failure for `MERGE` queries when `task_writer_count` is greater
  than one. ({issue}`14306`)

## Accumulo connector

* Add support for column comments when creating a new table. ({issue}`14114`)
* Move column mapping and index information into the output of `DESCRIBE`
  instead of a comment. ({issue}`14095`)

## BigQuery connector

* Fix improper escaping of backslash and newline characters. ({issue}`14254`)
* Fix query failure when the predicate involves a `varchar` value with a
  backslash. ({issue}`14254`)

## ClickHouse connector

* Upgrade minimum required Clickhouse version to 21.8. ({issue}`14112`)

## Delta Lake connector

* Improve performance when reading Parquet files for queries with predicates. ({issue}`14247`)

## Elasticsearch connector

* Deprecate support for query pass-through using the special
  `<index>$query:<es-query>` dynamic tables in favor of the `raw_query` table
  function. Legacy behavior can be re-enabled with the
  `elasticsearch.legacy-pass-through-query.enabled` configuration property. ({issue}`14015`)

## Hive connector

* Add support for partitioned views when legacy mode for view translation is
  enabled. ({issue}`14028`)
* Extend the `flush_metadata_cache` procedure to be able to flush table-related
  caches instead of only partition-related caches. ({issue}`14219`)
* Improve performance when reading Parquet files for queries with predicates. ({issue}`14247`)

## Iceberg connector

* Improve performance when reading Parquet files for queries with predicates. ({issue}`14247`)
* Fix potential table corruption when changing a table before it is known if
  committing to the Glue metastore has failed or succeeded. ({issue}`14174`)

## Pinot connector

* Add support for the `timestamp` type. ({issue}`10199`)

## SPI

* Extend `ConnectorMetadata.getStatisticsCollectionMetadata` to allow the
  connector to request the computation of any aggregation function during stats
  collection. ({issue}`14233`)
