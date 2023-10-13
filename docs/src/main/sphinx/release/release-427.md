# Release 427 (26 Sep 2023)

## General

* Add support for comparing IPv4 and IPv6 addresses and CIDRs with [contains](ip-address-contains). ({issue}`18497`)
* Improve performance of `GROUP BY` and `DISTINCT`. ({issue}`19059`)
* Reduce coordinator memory footprint when scannning tables. ({issue}`19009`)
* Fix failure due to exceeding node memory limits with `INSERT` statements. ({issue}`18771`)
* Fix query hang for certain `LIKE` patterns involving a mix of `%` and `_`. ({issue}`19146`)

## Security

* Ensure authorization is checked when accessing table comments with table redirections. ({issue}`18514`)

## Delta Lake connector

* Add support for reading tables with
  [Deletion Vectors](https://docs.delta.io/latest/delta-deletion-vectors.html). ({issue}`16903`)
* Add support for Delta Lake writer
  [version 7](https://docs.delta.io/latest/versioning.html#features-by-protocol-version). ({issue}`15873`)
* Add support for writing columns with the `timestamp(p)` type. ({issue}`16927`)
* Reduce data read from Parquet files for queries with filters. ({issue}`19032`)
* Improve performance of writing to Parquet files. ({issue}`19122`)
* Fix error reading Delta Lake table history when the initial transaction logs
  have been removed. ({issue}`18845`)

## Elasticsearch connector

* Fix query failure when a `LIKE` clause contains multi-byte characters. ({issue}`18966`)

## Hive connector

* Add support for changing column comments when using the Glue catalog. ({issue}`19076`)
* Reduce data read from Parquet files for queries with filters. ({issue}`19032`)
* Improve performance of reading text files. ({issue}`18959`)
* Allow changing a column's type from `double` to `varchar` in Hive tables. ({issue}`18930`)
* Remove legacy Hive readers and writers. The `*_native_reader_enabled` and
  `*_native_writer_enabled` session properties and `*.native-reader.enabled` and
  `*.native-writer.enabled` configuration properties are removed. ({issue}`18241`)
* Remove support for S3 Select. The `s3_select_pushdown_enabled` session
  property and the `hive.s3select*` configuration properties are removed. ({issue}`18241`)
* Remove support for disabling optimized symlink listing. The
  `optimize_symlink_listing` session property and
  `hive.optimize-symlink-listing` configuration property are removed. ({issue}`18241`)
* Fix incompatibility with Hive OpenCSV deserialization. As a result, when the
  escape character is explicitly set to `"`, a `\` (backslash) must be used
  instead. ({issue}`18918`)
* Fix performance regression when reading CSV files on AWS S3. ({issue}`18976`)
* Fix failure when creating a table with a `varchar(0)` column. ({issue}`18811`)

## Hudi connector

* Fix query failure when reading from Hudi tables with
  [`instants`](https://hudi.apache.org/docs/concepts/#timeline) that have been
  replaced. ({issue}`18213`)

## Iceberg connector

* Add support for usage of `date` and `timestamp` arguments in `FOR TIMESTAMP AS
  OF` expressions. ({issue}`14214`)
* Add support for using tags with `AS OF VERSION` queries. ({issue}`19111`)
* Reduce data read from Parquet files for queries with filters. ({issue}`19032`)
* Improve performance of writing to Parquet files. ({issue}`19090`)
* Improve performance of reading tables with many equality delete files. ({issue}`17114`)

## Ignite connector

* Add support for `UPDATE`. ({issue}`16445`)

## MariaDB connector

* Add support for `UPDATE`. ({issue}`16445`)

## MongoDB connector

* Fix query failure when mapping MongoDB `Decimal128` values with leading zeros. ({issue}`19068`)

## MySQL connector

* Add support for `UPDATE`. ({issue}`16445`)
* Change mapping for MySQL `TIMESTAMP` types from `timestamp(n)` to
  `timestamp(n) with time zone`. ({issue}`18470`)

## Oracle connector

* Add support for `UPDATE`. ({issue}`16445`)
* Fix potential query failure when joins are pushed down to Oracle. ({issue}`18924`)

## PostgreSQL connector

* Add support for `UPDATE`. ({issue}`16445`)

## Redshift connector

* Add support for `UPDATE`. ({issue}`16445`)

## SingleStore connector

* Add support for `UPDATE`. ({issue}`16445`)

## SQL Server connector

* Add support for `UPDATE`. ({issue}`16445`)

## SPI

* Change `BlockBuilder` to no longer extend `Block`. ({issue}`18738`)
