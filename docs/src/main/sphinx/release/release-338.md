# Release 338 (07 Jul 2020)

## General Changes

* Fix incorrect results when joining tables on a masked column. ({issue}`4251`)
* Fix planning failure when multiple columns have a mask. ({issue}`4322`)
* Fix incorrect comparison for `TIMESTAMP WITH TIME ZONE` values with precision larger than 3. ({issue}`4305`)
* Fix incorrect rounding for timestamps before 1970-01-01. ({issue}`4370`)
* Fix query failure when using `VALUES` with a floating point `NaN` value. ({issue}`4119`)
* Fix query failure when joining tables on a `real` or `double` column and one of the joined tables
  contains `NaN` value. ({issue}`4272`)
* Fix unauthorized error for internal requests to management endpoints. ({issue}`4304`)
* Fix memory leak while using dynamic filtering. ({issue}`4228`)
* Improve dynamic partition pruning for broadcast joins. ({issue}`4262`)
* Add support for setting column comments via the `COMMENT ON COLUMN` syntax. ({issue}`2516`)
* Add compatibility mode for legacy clients when rendering datetime type names with default precision
  in `information_schema` tables. This can be enabled via the `deprecated.omit-datetime-type-precision`
  configuration property or `omit_datetime_type_precision` session property. ({issue}`4349`, {issue}`4377`)
* Enforce `NOT NULL` column declarations when writing data. ({issue}`4144`)

## JDBC Driver Changes

* Fix excessive CPU usage when reading query results. ({issue}`3928`)
* Implement `DatabaseMetaData.getClientInfoProperties()`. ({issue}`4318`)

## Elasticsearch Connector Changes

* Add support for reading numeric values encoded as strings. ({issue}`4341`)

## Hive Connector Changes

* Fix incorrect query results when Parquet file has no min/max statistics for an integral column. ({issue}`4200`)
* Fix query failure when reading from a table partitioned on a `real` or `double` column containing
  a `NaN` value. ({issue}`4266`)
* Fix sporadic failure when writing to bucketed sorted tables on S3. ({issue}`2296`)
* Fix handling of strings when translating Hive views. ({issue}`3266`)
* Do not require cache directories to be configured on coordinator. ({issue}`3987`, {issue}`4280`)
* Fix Azure ADL caching support. ({issue}`4240`)
* Add support for setting column comments. ({issue}`2516`)
* Add hidden `$partition` column for partitioned tables that contains the partition name. ({issue}`3582`)

## Kafka Connector Changes

* Fix query failure when a column is projected and also referenced in a query predicate
  when reading from Kafka topic using `RAW` decoder. ({issue}`4183`)

## MySQL Connector Changes

* Fix type mapping for unsigned integer types. ({issue}`4187`)

## Oracle Connector Changes

* Exclude internal schemas (e.g., sys) from schema listings. ({issue}`3784`)
* Add support for connection pooling. ({issue}`3770`)

## Base-JDBC Connector Library Changes

* Exclude the underlying database's `information_schema` from schema listings. ({issue}`3834`)
