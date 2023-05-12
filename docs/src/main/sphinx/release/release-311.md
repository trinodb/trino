# Release 311 (14 May 2019)

## General

- Fix incorrect results for aggregation query that contains a `HAVING` clause but no
  `GROUP BY` clause. ({issue}`733`)
- Fix rare error when moving already completed query to a new memory pool. ({issue}`725`)
- Fix leak in operator peak memory computations ({issue}`764`)
- Improve consistency of reported query statistics. ({issue}`773`)
- Add support for `OFFSET` syntax. ({issue}`732`)
- Print cost metrics using appropriate units in the output of `EXPLAIN`. ({issue}`68`)
- Add {func}`combinations` function. ({issue}`714`)

## Hive connector

- Add support for static AWS credentials for the Glue metastore. ({issue}`748`)

## Cassandra connector

- Support collections nested in other collections. ({issue}`657`)
- Automatically discover the Cassandra protocol version when the previously required
  `cassandra.protocol-version` configuration property is not set. ({issue}`596`)

## Black Hole connector

- Fix rendering of tables and columns in plans. ({issue}`728`)
- Add table and column statistics. ({issue}`728`)

## System connector

- Add `system.metadata.table_comments` table that contains table comments. ({issue}`531`)
