# Release 419 (5 Jun 2023)

## General

* Add the {func}`array_histogram` function to find the number of occurrences of
  the unique elements in an array. ({issue}`14725 `)
* Improve planning performance for queries involving joins. ({issue}`17458`)
* Fix query failure when the server JSON response exceeds the 5MB limit for
  string values. ({issue}`17557`)

## Web UI

* Allow uppercase or mixed case values for the `web-ui.authentication.type`
  configuration property. ({issue}`17334`)

## BigQuery connector

* Add support for proxying BigQuery APIs via an HTTP(S) proxy. ({issue}`17508`)
* Improve performance of retrieving metadata from BigQuery. ({issue}`16064`)

## Delta Lake connector

* Support the `id` and `name` mapping modes when adding new columns. ({issue}`17236`)
* Improve performance of reading Parquet files. ({issue}`17612`)
* Improve performance when writing Parquet files with
  [structural data types](structural-data-types). ({issue}`17665`)
* Properly display the schema, table name, and location of tables being inserted
  into in the output of `EXPLAIN` queries. ({issue}`17590`)
* Fix query failure when writing to a file location with a trailing `/` in its
  name. ({issue}`17552`)

## Hive connector

* Add support for reading ORC files with shorthand timezone ids in the Stripe
  footer metadata. You can set the `hive.orc.read-legacy-short-zone-id`
  configuration property to `true` to enable this behavior. ({issue}`12303`)
* Improve performance of reading ORC files with Bloom filter indexes. ({issue}`17530`)
* Improve performance of reading Parquet files. ({issue}`17612`)
* Improve optimized Parquet writer performance for
  [structural data types](structural-data-types). ({issue}`17665`)
* Fix query failure for tables with file paths that contain non-alphanumeric
  characters. ({issue}`17621`)

## Hudi connector

* Improve performance of reading Parquet files. ({issue}`17612`)
* Improve performance when writing Parquet files with
  [structural data types](structural-data-types). ({issue}`17665`)

## Iceberg connector

* Add support for the [Nessie catalog](iceberg-nessie-catalog). ({issue}`11701`)
* Disallow use of the `migrate` table procedure on Hive tables with `array`,
  `map` and `row` types. Previously, this returned incorrect results after the
  migration. ({issue}`17587`)
* Improve performance of reading ORC files with Bloom filter indexes. ({issue}`17530`)
* Improve performance of reading Parquet files. ({issue}`17612`)
* Improve performance when writing Parquet files with
  [structural data types](structural-data-types). ({issue}`17665`)
* Improve performance of reading table statistics. ({issue}`16745`)

## SPI

* Remove unused `NullAdaptationPolicy` from `ScalarFunctionAdapter`. ({issue}`17706`)
