# Release 439 (15 Feb 2024)

## General

* Fix failure when setting session properties for a catalog with a `.` in its
  name. ({issue}`20474`)
* Fix potential out-of-memory query failures when using the experimental scheduler. ({issue}`20694`)
* Fix potential performance regression when dynamic filters are not applied. ({issue}`20709`)

## BigQuery connector

* Fix failure when pushing down predicates into BigQuery views. ({issue}`20627`)

## Delta Lake connector

* Improve performance when reading data by adding support for
  [caching data on local storage](/object-storage/file-system-cache). ({issue}`18719`)
* Fix potential crash when reading corrupted Snappy data. ({issue}`20631`)

## Hive connector

* {{breaking}} Improve performance of caching data on local storage. Deprecate
  the `hive.cache.enabled` configuration property in favor of 
  [`fs.cache.enabled`](/object-storage/file-system-cache). ({issue}`20658`, {issue}`20102`)
* Fix query failure when a value has not been specified for the
  `orc_bloom_filter_fpp` table property. ({issue}`16589`)
* Fix potential query failure when writing ORC files. ({issue}`20587`)
* Fix potential crash when reading corrupted Snappy data. ({issue}`20631`)

## Hudi connector

* Fix potential crash when reading corrupted Snappy data. ({issue}`20631`)

## Iceberg connector

* Improve performance when reading data by adding support for
  [caching data on local storage](/object-storage/file-system-cache). ({issue}`20602`)
* Fix query failure when a value has not been specified for the
  `orc_bloom_filter_fpp` table property. ({issue}`16589`)
* Fix potential query failure when writing ORC files. ({issue}`20587`)
* Fix potential crash when reading corrupted Snappy data. ({issue}`20631`)

## Redshift connector

* Fix potential crash when reading corrupted Snappy data. ({issue}`20631`)
