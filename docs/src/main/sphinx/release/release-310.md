# Release 310 (3 May 2019)

## General

- Reduce compilation failures for expressions over types containing an extremely
  large number of nested types. ({issue}`537`)
- Fix error reporting when query fails with due to running out of memory. ({issue}`696`)
- Improve performance of `JOIN` queries involving join keys of different types.
  ({issue}`665`)
- Add initial and experimental support for late materialization.
  This feature can be enabled via `experimental.work-processor-pipelines`
  feature config or via `work_processor_pipelines` session config.
  Simple select queries of type `SELECT ... FROM table ORDER BY cols LIMIT n` can
  experience significant CPU and performance improvement. ({issue}`602`)
- Add support for `FETCH FIRST` syntax. ({issue}`666`)

## CLI

- Make the final query time consistent with query stats. ({issue}`692`)

## Hive connector

- Ignore boolean column statistics when the count is `-1`. ({issue}`241`)
- Prevent failures for `information_schema` queries when a table has an invalid
  storage format. ({issue}`568`)
- Add support for assuming AWS role when accessing S3 or Glue. ({issue}`698`)
- Add support for coercions between `DECIMAL`, `DOUBLE`, and `REAL` for
  partition and table schema mismatch. ({issue}`352`)
- Fix typo in Metastore recorder duration property name. ({issue}`711`)

## PostgreSQL connector

- Support for the `ARRAY` type has been disabled by default.  ({issue}`687`)

## Blackhole connector

- Support having tables with same name in different Blackhole schemas. ({issue}`550`)
