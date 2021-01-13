# Release 348 (14 Dec 2020)

## General

* Add support for `DISTINCT` clause in aggregations within correlated subqueries. ({issue}`5904`)
* Support `SHOW STATS` for arbitrary queries. ({issue}`3109`)
* Improve query performance by reducing worker to worker communication overhead. ({issue}`6126`)
* Improve performance of `ORDER BY ... LIMIT` queries. ({issue}`6072`)
* Reduce memory pressure and improve performance of queries involving joins. ({issue}`6176`)
* Fix ``EXPLAIN ANALYZE`` for certain queries that contain broadcast join. ({issue}`6115`)
* Fix planning failures for queries that contain outer joins and aggregations using `FILTER (WHERE <condition>)` syntax. ({issue}`6141`)
* Fix incorrect results when correlated subquery in join contains aggregation functions such as `array_agg` or `checksum`. ({issue}`6145`)
* Fix incorrect query results when using `timestamp with time zone` constants with precision higher than 3
  describing same point in time but in different zones. ({issue}`6318`)
* Fix duplicate query completion events if query fails early. ({issue}`6103`)
* Fix query failure when views are accessed and current session does not
  specify default schema and catalog. ({issue}`6294`)

## Web UI

* Add support for OAuth2 authorization. ({issue}`5355`)
* Fix invalid operator stats in Stage Performance view. ({issue}`6114`)

## JDBC driver

* Allow reading `timestamp with time zone` value as `ZonedDateTime` using `ResultSet.getObject(int column, Class<?> type)` method. ({issue}`307`)
* Accept `java.time.LocalDate` in `PreparedStatement.setObject(int, Object)`. ({issue}`6301`)
* Extend `PreparedStatement.setObject(int, Object, int)` to allow setting `time` and `timestamp` values with precision higher than nanoseconds. ({issue}`6300`)
  This can be done via providing a `String` value representing a valid SQL literal.
* Change representation of a `row` value. `ResultSet.getObject` now returns an instance of `io.prestosql.jdbc.Row` class, which better represents
  the returned value. Previously a `row` value was represented as a `Map` instance, with unnamed fields being named like `field0`, `field1`, etc.
  You can access the previous behavior by invoking `getObject(column, Map.class)` on the `ResultSet` object. ({issue}`4588`)
* Represent `varbinary` value using hex string representation in `ResultSet.getString`. Previously the return value was useless, similar to `"B@2de82bf8"`. ({issue}`6247`)
* Report precision of the `time(p)`, `time(p) with time zone`,  `timestamp(p)` and `timestamp(p) with time zone` in the `DECIMAL_DIGITS` column
  in the result set returned from `DatabaseMetaData#getColumns`. ({issue}`6307`)
* Fix the value of the `DATA_TYPE` column for `time(p)` and `time(p) with time zone` in the result set returned from `DatabaseMetaData#getColumns`.  ({issue}`6307`)
* Fix failure when reading a `timestamp` or `timestamp with time zone` value with seconds fraction greater than or equal to 999999999500 picoseconds. ({issue}`6147`)
* Fix failure when reading a `time` value with seconds fraction greater than or equal to 999999999500 picoseconds. ({issue}`6204`)
* Fix element representation in arrays returned from `ResultSet.getArray`, making it consistent with `ResultSet.getObject`.
  Previously the elements were represented using internal client representation (e.g. `String`). ({issue}`6048`)
* Fix `ResultSetMetaData.getColumnType` for `timestamp with time zone`. Previously the type was miscategorized as `java.sql.Types.TIMESTAMP`. ({issue}`6251`)
* Fix `ResultSetMetaData.getColumnType` for `time with time zone`. Previously the type was miscategorized as `java.sql.Types.TIME`. ({issue}`6251`)
* Fix failure when an instance of `SphericalGeography` geospatial type is returned in the `ResultSet`. ({issue}`6240`)

## CLI

* Fix rendering of `row` values with unnamed fields. Previously they were printed using fake field names like `field0`, `field1`, etc. ({issue}`4587`)
* Fix query progress reporting. ({issue}`6119`)
* Fix failure when an instance of `SphericalGeography` geospatial type is returned to the client. ({issue}`6238`)

## Hive connector

* Allow configuring S3 endpoint in security mapping. ({issue}`3869`)
* Add support for S3 streaming uploads. Data is uploaded to S3 as it is written, rather
  than staged to a local temporary file. This feature is disabled by default, and can be enabled
  using the `hive.s3.streaming.enabled` configuration property. ({issue}`3712`, {issue}`6201`)
* Reduce load on metastore when background cache refresh is enabled. ({issue}`6101`, {issue}`6156`)
* Verify that data is in the correct bucket file when reading bucketed tables.
  This is enabled by default, as incorrect bucketing can cause incorrect query results,
  but can be disabled using the `hive.validate-bucketing` configuration property
  or the `validate_bucketing` session property. ({issue}`6012`)
* Allow fallback to legacy Hive view translation logic via `hive.legacy-hive-view-translation` config property or
  `legacy_hive_view_translation` session property. ({issue}`6195 `)
* Add deserializer class name to split information exposed to the event listener. ({issue}`6006`)
* Improve performance when querying tables that contain symlinks. ({issue}`6158`, {issue}`6213`)

## Iceberg connector

* Improve performance of queries containing filters on non-partition columns. Such filters are now used
  for optimizing split generation and table scan.  ({issue}`4932`)
* Add support for Google Cloud Storage and Azure Storage. ({issue}`6186`)

## Kafka connector

* Allow writing `timestamp with time zone` values into columns using `milliseconds-since-epoch` or
  `seconds-since-epoch` JSON encoders. ({issue}`6074`)

## Other connectors

* Fix ineffective table metadata caching for PostgreSQL, MySQL, SQL Server, Redshift, MemSQL and Phoenix connectors. ({issue}`6081`, {issue}`6167`)

## SPI

* Change `SystemAccessControl#filterColumns` and `ConnectorAccessControl#filterColumns` methods to accept a set of
  column names, and return a set of visible column names. ({issue}`6084`)
* Expose catalog names corresponding to the splits through the split completion event of the event listener. ({issue}`6006`)
