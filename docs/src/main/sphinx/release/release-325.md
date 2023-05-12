# Release 325 (14 Nov 2019)

:::{warning}
There is a performance regression in this release.
:::

## General

- Fix incorrect results for certain queries involving `FULL` or `RIGHT` joins and
  `LATERAL`. ({issue}`1952`)
- Fix incorrect results when using `IS DISTINCT FROM` on columns of `DECIMAL` type
  with precision larger than 18. ({issue}`1985`)
- Fix query failure when row types contain a field named after a reserved SQL keyword. ({issue}`1963`)
- Add support for `LIKE` predicate to `SHOW SESSION` and `SHOW FUNCTIONS`. ({issue}`1688`, {issue}`1692`)
- Add support for late materialization to join operations. ({issue}`1256`)
- Reduce number of metadata queries during planning.
  This change disables stats collection for non-`EXPLAIN` queries. If you
  want to have access to such stats and cost in query completion events, you
  need to re-enable stats collection using the `collect-plan-statistics-for-all-queries`
  configuration property. ({issue}`1866`)
- Add variant of {func}`strpos` that returns the Nth occurrence of a substring. ({issue}`1811`)
- Add {func}`to_encoded_polyline` and {func}`from_encoded_polyline` geospatial functions. ({issue}`1827`)

## Web UI

- Show actual query for an `EXECUTE` statement. ({issue}`1980`)

## Hive

- Fix incorrect behavior of `CREATE TABLE` when Hive metastore is configured
  with `metastore.create.as.acid` set to `true`. ({issue}`1958`)
- Fix query failure when reading Parquet files that contain character data without statistics. ({issue}`1955`)
- Allow analyzing a subset of table columns (rather than all columns). ({issue}`1907`)
- Support overwriting unpartitioned tables for insert queries when using AWS Glue. ({issue}`1243`)
- Add support for reading Parquet files where the declared precision of decimal columns does not match
  the precision in the table or partition schema. ({issue}`1949`)
- Improve performance when reading Parquet files with small row groups. ({issue}`1925`)

## Other connectors

These changes apply to the MySQL, PostgreSQL, Redshift, and SQL Server connectors.

- Fix incorrect insertion of data when the target table has an unsupported type. ({issue}`1930`)
