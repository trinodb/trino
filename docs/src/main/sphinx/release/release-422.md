# Release 422 (13 Jul 2023)

## General

* Add support for adding nested fields with an `ADD COLUMN` statement. ({issue}`16248`)
* Improve performance of `INSERT` and `CREATE TABLE AS ... SELECT` queries. ({issue}`18005`)
* Prevent queries from hanging when worker nodes fail and the
  `task.retry-policy` configuration property is set to `TASK`. ({issue}`18175 `)

## Security

* Add support for validating JWT types with OAuth 2.0 authentication. ({issue}`17640`)
* Fix error when the `http-server.authentication.type` configuration property
  is set to `oauth2` or `jwt` and the `principal-field` property's value
  differs. ({issue}`18210`)

## BigQuery connector

* Add support for writing to columns with a `timestamp(p) with time zone` type. ({issue}`17793`)

## Delta Lake connector

* Add support for renaming columns. ({issue}`15821`)
* Improve performance of reading from tables with a large number of
  [checkpoints](https://docs.delta.io/latest/delta-batch.html#-data-retention). ({issue}`17405`)
* Disallow using the `vacuum` procedure when the max
  [writer version](https://docs.delta.io/latest/versioning.html#features-by-protocol-version)
  is above 5. ({issue}`18095`)

## Hive connector

* Add support for reading the `timestamp with local time zone` Hive type. ({issue}`1240`)
* Add a native Avro file format writer. This can be disabled with the
  `avro.native-writer.enabled` configuration property or the
  `avro_native_writer_enabled` session property. ({issue}`18064`)
* Fix query failure when the `hive.recursive-directories` configuration property
  is set to true and partition names contain non-alphanumeric characters. ({issue}`18167`)
* Fix incorrect results when reading text and `RCTEXT` files with a value that
  contains the character that separates fields. ({issue}`18215`)
* Fix incorrect results when reading concatenated `GZIP` compressed text files. ({issue}`18223`)
* Fix incorrect results when reading large text and sequence files with a single
  header row. ({issue}`18255`)
* Fix incorrect reporting of bytes read for compressed text files. ({issue}`1828`)

## Iceberg connector

* Add support for adding nested fields with an `ADD COLUMN` statement. ({issue}`16248`)
* Add support for the `register_table` procedure to register Hadoop tables. ({issue}`16363`)
* Change the default file format to Parquet. The `iceberg.file-format`
  catalog configuration property can be used to specify a different default file
  format. ({issue}`18170`)
* Improve performance of reading `row` types from Parquet files. ({issue}`17387`)
* Fix failure when writing to tables sorted on `UUID` or `TIME` types. ({issue}`18136`)

## Kudu connector

* Add support for table comments when creating tables. ({issue}`17945`)

## Redshift connector

* Prevent returning incorrect results by throwing an error when encountering
  unsupported types. Previously, the query would fall back to the legacy type
  mapping. ({issue}`18209`)
