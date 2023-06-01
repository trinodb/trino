# Release 319 (22 Sep 2019)

## General

- Fix planning failure for queries involving `UNION` and `DISTINCT` aggregates. ({issue}`1510`)
- Fix excessive runtime when parsing expressions involving `CASE`. ({issue}`1407`)
- Fix fragment output size in `EXPLAIN ANALYZE` output. ({issue}`1345`)
- Fix a rare failure when running `EXPLAIN ANALYZE` on a query containing
  window functions. ({issue}`1401`)
- Fix failure when querying `/v1/resourceGroupState` endpoint for non-existing resource
  group. ({issue}`1368`)
- Fix incorrect results when reading `information_schema.table_privileges` with
  an equality predicate on `table_name` but without a predicate on `table_schema`.
  ({issue}`1534`)
- Fix planning failure due to coercion handling for correlated subqueries. ({issue}`1453`)
- Improve performance of queries against `information_schema` tables. ({issue}`1329`)
- Reduce metadata querying during planning. ({issue}`1308`, {issue}`1455`)
- Improve performance of certain queries involving coercions and complex expressions in `JOIN`
  conditions. ({issue}`1390`)
- Include cost estimates in output of `EXPLAIN (TYPE IO)`. ({issue}`806`)
- Improve support for correlated subqueries involving `ORDER BY` or `LIMIT`. ({issue}`1415`)
- Improve performance of certain `JOIN` queries when automatic join ordering is enabled. ({issue}`1431`)
- Allow setting the default session catalog and schema via the `sql.default-catalog`
  and `sql.default-schema` configuration properties. ({issue}`1524`)
- Add support for `IGNORE NULLS` for window functions. ({issue}`1244`)
- Add support for `INNER` and `OUTER` joins involving `UNNEST`. ({issue}`1522`)
- Rename `legacy` and `flat` {doc}`scheduler policies </admin/properties-node-scheduler>` to
  `uniform` and `topology` respectively.  These can be configured via the `node-scheduler.policy`
  configuration property. ({issue}`10491`)
- Add `file` {doc}`network topology provider </admin/properties-node-scheduler>` which can be configured
  via the `node-scheduler.network-topology.type` configuration property. ({issue}`1500`)
- Add support for `SphericalGeography` to {func}`ST_Length`. ({issue}`1551`)

## Security

- Allow configuring read-only access in {doc}`/security/built-in-system-access-control`. ({issue}`1153`)
- Add missing checks for schema create, rename, and drop in file-based `SystemAccessControl`. ({issue}`1153`)
- Allow authentication over HTTP for forwarded requests containing the
  `X-Forwarded-Proto` header. This is disabled by default, but can be enabled using the
  `http-server.authentication.allow-forwarded-https` configuration property. ({issue}`1442`)

## Web UI

- Fix rendering bug in Query Timeline resulting in inconsistency of presented information after
  query finishes. ({issue}`1371`)
- Show total memory in Query Timeline instead of user memory. ({issue}`1371`)

## CLI

- Add `--insecure` option to skip validation of server certificates for debugging. ({issue}`1484`)

## Hive connector

- Fix reading from `information_schema`, as well as `SHOW SCHEMAS`, `SHOW TABLES`, and
  `SHOW COLUMNS` when connecting to a Hive 3.x metastore that contains an `information_schema`
  schema. ({issue}`1192`)
- Improve performance when reading data from GCS. ({issue}`1443`)
- Allow accessing tables in Glue metastore that do not have a table type. ({issue}`1343`)
- Add support for Azure Data Lake (`adl`) file system. ({issue}`1499`)
- Allow using custom S3 file systems by relying on the default Hadoop configuration by specifying
  `HADOOP_DEFAULT` for the `hive.s3-file-system-type` configuration property. ({issue}`1397`)
- Add support for instance credentials for the Glue metastore via the
  `hive.metastore.glue.use-instance-credentials` configuration property. ({issue}`1363`)
- Add support for custom credentials providers for the Glue metastore via the
  `hive.metastore.glue.aws-credentials-provider` configuration property. ({issue}`1363`)
- Do not require setting the `hive.metastore-refresh-interval` configuration property
  when enabling metastore caching. ({issue}`1473`)
- Add `textfile_field_separator` and `textfile_field_separator_escape` table properties
  to support custom field separators for `TEXTFILE` format tables. ({issue}`1439`)
- Add `$file_size` and `$file_modified_time` hidden columns. ({issue}`1428`)
- The `hive.metastore-timeout` configuration property is now accepted only when using the
  Thrift metastore. Previously, it was accepted for other metastore type, but was
  ignored. ({issue}`1346`)
- Disallow reads from transactional tables. Previously, reads would appear to work,
  but would not return any data. ({issue}`1218`)
- Disallow writes to transactional tables. Previously, writes would appear to work,
  but the data would be written incorrectly. ({issue}`1218`)
