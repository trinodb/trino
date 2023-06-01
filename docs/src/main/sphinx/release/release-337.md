# Release 337 (25 Jun 2020)

:::{Note}
This release fixes a potential security vulnerability when secure internal communication is enabled in a cluster. A malicious
attacker can take advantage of this vulnerability to escalate privileges to internal APIs. We encourage everyone to upgrade as soon
as possible.
:::

## General

- Fix incorrect results for inequality join involving `NaN`. ({issue}`4120`)
- Fix peak non-revocable memory metric in event listener. ({issue}`4096`)
- Fix queued query JMX stats. ({issue}`4129`)
- Fix rendering of types in the output of `DESCRIBE INPUT`. ({issue}`4023`)
- Improve performance of queries involving comparisons between `DOUBLE` or `REAL` values and integer values. ({issue}`3533`)
- Reduce idle CPU consumption in coordinator. ({issue}`3990`)
- Add peak non-revocable memory metric to query stats. ({issue}`4096`)
- Add support for variable-precision `TIMESTAMP WITH TIME ZONE` type ({issue}`3947`)
- Add support for `IN` predicate with subqueries in outer join condition. ({issue}`4151`)
- Add support for quantified comparisons (e.g., `> ALL (...)`) in aggregation queries. ({issue}`4128`)
- Add {doc}`/connector/druid`. ({issue}`3522`)
- Add {func}`translate` function. ({issue}`4080`)
- Reduce worker graceful shutdown duration. ({issue}`4192`)

## Security

- Disable insecure authentication over HTTP by default when HTTPS with authentication is enabled. This
  can be overridden via the `http-server.authentication.allow-insecure-over-http` configuration property. ({issue}`4199`)
- Add support for insecure authentication over HTTPS to the Web UI. ({issue}`4199`)
- Add {ref}`system-file-auth-system-information` which control the ability of a
  user to access to read and write system management information.
  ({issue}`4199`)
- Disable user impersonation in default system security. ({issue}`4082`)

## Elasticsearch connector

- Add support for password authentication. ({issue}`4165`)

## Hive connector

- Fix reading CSV tables with `separatorChar`, `quoteChar` or `escapeChar` table property
  containing more than one character. For compatibility with Hive, only first character is considered
  and remaining are ignored. ({issue}`3891`)
- Improve performance of `INSERT` queries writing to bucketed tables when some buckets do not contain any data. ({issue}`1375`)
- Improve performance of queries reading Parquet data with predicates on `timestamp` columns. ({issue}`4104`)
- Improve performance for join queries over partitioned tables. ({issue}`4156`)
- Add support for `null_format` table property for tables using TextFile storage format ({issue}`4056`)
- Add support for `null_format` table property for tables using RCText and SequenceFile
  storage formats ({issue}`4143`)
- Add optimized Parquet writer. The new writer is disabled by default, and can be enabled with the
  `parquet_optimized_writer_enabled` session property or the `hive.parquet.optimized-writer.enabled` configuration
  property. ({issue}`3400`)
- Add support caching data in Azure Data Lake and AliyunOSS storage. ({issue}`4213`)
- Fix failures when caching data from Google Cloud Storage. ({issue}`4213`)
- Support ACID data files naming used when direct inserts are enabled in Hive (HIVE-21164).
  Direct inserts is an upcoming feature in Hive 4. ({issue}`4049`)

## PostgreSQL connector

- Improve performance of aggregation queries by computing aggregations within PostgreSQL database.
  Currently, the following aggregate functions are eligible for pushdown:
  `count`,  `min`, `max`, `sum` and `avg`. ({issue}`3881`)

## Base-JDBC connector library

- Implement framework for aggregation pushdown. ({issue}`3881`)
