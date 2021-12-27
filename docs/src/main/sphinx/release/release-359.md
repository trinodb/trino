# Release 359 (1 Jul 2021)

## General

* Raise minimum required Java version for running Trino server to 11.0.11. ({issue}`8103`)
* Add support for row pattern recognition in window specification. ({issue}`8141`)
* Add support for {doc}`/sql/set-time-zone`. ({issue}`8112`)
* Add {func}`geometry_nearest_points`. ({issue}`8280`)
* Add {func}`current_groups`. ({issue}`8446`)
* Add support for `varchar`, `varbinary` and `date` types to {func}`make_set_digest`. ({issue}`8295`)
* Add support for granting `UPDATE` privileges. ({issue}`8279`)
* List materialized view columns in the `information_schema.columns` table. ({issue}`8113`)
* Expose comments in views and materialized views in `system.metadata.table_comments` correctly. ({issue}`8327`)
* Fix query failure for certain queries with `ORDER BY ... LIMIT` on sorted data. ({issue}`8184`)
* Fix incorrect query results for certain queries using `LIKE` with pattern against
  `char` columns in the `WHERE` clause. ({issue}`8311`)
* Fix planning failure when using {func}`hash_counts`. ({issue}`8248`)
* Fix error message when grouping expressions in `GROUP BY` queries contain aggregations, window functions or grouping operations. ({issue}`8247`)

## Security

* Fix spurious impersonation check when applying user mapping for password authentication. ({issue}`7027`)
* Fix handling of multiple LDAP user bind patterns. ({issue}`8134`)

## Web UI

* Show session timezone in query details page. ({issue}`4196`)

## Docker image

* Add support for ARM64. ({issue}`8397`)

## CLI

* Add support for logging of network traffic via the `--network-logging` command line option. ({issue}`8329`)

## BigQuery connector

* Add `bigquery.views-cache-ttl` config property to allow configuring the cache expiration for BigQuery views. ({issue}`8236`)
* Fix incorrect results when accessing BigQuery records with wrong index. ({issue}`8183`)

## Elasticsearch connector

* Fix potential incorrect results when queries contain an `IS NULL` predicate. ({issue}`3605`)
* Fix failure when multiple indexes share the same alias. ({issue}`8158`)

## Hive connector

* Rename `hive-hadoop2` connector to `hive`. ({issue}`8166`)
* Add support for Hive views which use `GROUP BY` over a subquery that also uses `GROUP BY` on matching columns. ({issue}`7635`)
* Add support for granting `UPDATE` privileges when `hive.security=sql-standard` is used. ({issue}`8279`)
* Add support for inserting data into CSV and TEXT tables with `skip_header_line_count` table property set to 1.
  The same applies to creating tables with data using `CREATE TABLE ... AS SELECT` syntax. ({issue}`8390`)
* Disallow creating CSV and TEXT tables with data if `skip_header_line_count` is set to a value
  greater than 0. ({issue}`8373`)
* Fix query failure when reading from a non-ORC insert-only transactional table. ({issue}`8259`)
* Fix incorrect results when reading ORC ACID tables containing deleted rows. ({issue}`8208`)
* Respect `hive.metastore.glue.get-partition-threads` configuration property. ({issue}`8320`)

## Iceberg connector

* Do not include Hive views in `SHOW TABLES` query results. ({issue}`8153`)

## MongoDB connector

* Skip creating an index for the `_schema` collection if it already exists. ({issue}`8264`)

## MySQL connector

* Support reading and writing `timestamp` values with precision higher than 3. ({issue}`6910`)
* Support predicate pushdown on `timestamp` columns. ({issue}`7413`)
* Handle `timestamp` values during forward offset changes ('gaps' in DST) correctly. ({issue}`5449`)

## SPI

* Introduce `ConnectorMetadata#listMaterializedViews` for listing materialized view names. ({issue}`8113`)
* Introduce `ConnectorMetadata#getMaterializedViews` for getting materialized view definitions. ({issue}`8113`)
* Enable connector to delegate materialized view refresh to itself. ({issue}`7960`)
* Allow computing HyperLogLog based approximate set summary as a column statistic during `ConnectorMetadata`
  driven statistics collection flow. ({issue}`8355`)
* Report output column types through `EventListener`. ({issue}`8405`)
* Report input column information for queries involving set operations (`UNION`, `INTERSECT` and `EXCEPT`). ({issue}`8371`)
