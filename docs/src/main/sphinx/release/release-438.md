# Release 438 (1 Feb 2024)

## General

* Add support for using types such as `char`, `varchar`, `uuid`, `ip_address`,
  `geometry`, and others with the {func}`reduce_agg` function. ({issue}`20452`)
* Fix query failure when using `char` types with the {func}`reverse` function. ({issue}`20387`)
* Fix potential query failure when using the {func}`max_by` function on large
  datasets. ({issue}`20524`)
* Fix query failure when querying data with deeply nested rows. ({issue}`20529`)

## Security

* Add support for access control with
  [Open Policy Agent](/security/opa-access-control). ({issue}`19532`)

## Delta Lake connector

* Add support for configuring the maximum number of values per page when writing
  to Parquet files with the `parquet.writer.page-value-count` configuration
  property or the `parquet_writer_page_value_count` session property. ({issue}`20171`)
* Add support for `ALTER COLUMN ... DROP NOT NULL` statements. ({issue}`20448`)

## Hive connector

* Add support for configuring the maximum number of values per page when writing
  to Parquet files with the `parquet.writer.page-value-count` configuration
  property or the `parquet_writer_page_value_count` session property. ({issue}`20171`)

## Iceberg connector

* Add support for `ALTER COLUMN ... DROP NOT NULL` statements. ({issue}`20315`)
* Add support for configuring the maximum number of values per page when writing
  to Parquet files with the `parquet.writer.page-value-count` configuration
  property or the `parquet_writer_page_value_count` session property. ({issue}`20171`)
* Add support for `array`, `map` and `row` types in the `migrate` table
  procedure. ({issue}`17583`)

## Pinot connector

* Add support for the `date` type. ({issue}`13059`)

## PostgreSQL connector

* Add support for `ALTER COLUMN ... DROP NOT NULL` statements. ({issue}`20315`)
