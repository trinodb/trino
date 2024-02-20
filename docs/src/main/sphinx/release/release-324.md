# Release 324 (1 Nov 2019)

## General

- Fix query failure when `CASE` operands have different types. ({issue}`1825`)
- Add support for `ESCAPE` clause in `SHOW CATALOGS LIKE ...`. ({issue}`1691`)
- Add {func}`line_interpolate_point` and {func}`line_interpolate_points`. ({issue}`1888`)
- Allow references to tables in the enclosing query when using `.*`. ({issue}`1867`)
- Configuration properties for optimizer and spill support no longer
  have `experimental.` prefix. ({issue}`1875`)
- Configuration property `experimental.reserved-pool-enabled` was renamed to
  `experimental.reserved-pool-disabled` (with meaning reversed). ({issue}`1916`)

## Security

- Perform access control checks when displaying table or view definitions
  with `SHOW CREATE`. ({issue}`1517`)

## Hive

- Allow using `SHOW GRANTS` on a Hive view when using the `sql-standard`
  security mode. ({issue}`1842`)
- Improve performance when filtering dictionary-encoded Parquet columns. ({issue}`1846`)

## PostgreSQL

- Add support for inserting `MAP(VARCHAR, VARCHAR)` values into columns of
  `hstore` type. ({issue}`1894`)

## Elasticsearch

- Fix failure when reading datetime columns in Elasticsearch 5.x. ({issue}`1844`)
- Add support for mixed-case field names. ({issue}`1914`)

## SPI

- Introduce a builder for `ColumnMetadata`. The various overloaded constructors
  are now deprecated. ({issue}`1891`)
