# Product Test Audit Progress

This file tracks the literal manual source-body semantic audit.

STOP RULE:
- Checkpoint commits are recovery-only and are never a reason to stop work or report completion.
- Do not stop or claim completion until every lane and every suite in this file is marked `complete`, unless a true blocker is recorded in this file.
- If any lane or suite is not `complete`, the audit is still in progress and work must continue.

Status meanings:
- `not started`: the semantic comparison has not yet been performed for this lane or suite in this final pass.
- `in progress`: the semantic comparison is actively being rewritten from legacy/current source bodies and may be partial.
- `complete`: the semantic comparison was performed manually from source and recorded in the audit files.

Global state:
- Mechanical inventory reconciliation: complete.
- Exhaustive source/history reference validation: complete.
  - Legacy method references checked against `upstream/master` merge-base: `1092`
  - Current method references checked against the current tree: `1273`
  - Broken legacy/current method references found: `0`
- Current code tree state for this audit pass: recorded gap-restoration code changes plus matching audit-doc updates.
- Final lane and suite semantic pass: complete.
- Known unresolved fidelity gaps currently recorded in this tracker: none.
- Intentionally retired legacy-only methods:
  - the three retired `TestSqlCancel` methods

Stop failure analysis:
- The current stop failure mode was not checkpoints; it was incorrectly treating older detailed baseline lane prose as
  sufficient completion evidence after lighter revalidation of suite/environment wiring.
- That shortcut was removed by finishing the remaining fresh final-pass rereads for Iceberg and Delta Lake before
  restoring this tracker to `complete`.

Remaining mandatory task list before any completion claim:
1. None. Iceberg and Delta Lake were freshly re-read one method at a time from legacy/current source bodies in the
   final pass.

Completion gate:
- All lane rows below must show:
  - `method status = complete`
  - `environment status = complete`
  - `suite status = complete`
- All suite rows below must show:
  - `suite audit status = complete`
- If any one of these conditions is false, the audit is not done.

Mandatory pre-stop checklist:
- No remaining lane or suite may still be `in progress`.
- No lane or suite note may rely on wording like `revalidated` as its primary completion evidence.
- `README.md` and this file must both agree on whether the audit is still in progress.
- If a blocker exists, it must be written in this file and the exact stopped state must be written here before stopping.

## Lane Progress

| Lane | Method status | Environment status | Suite status | Open findings | Note |
| --- | --- | --- | --- | --- | --- |
| `mysql-mariadb` | `complete` | `complete` | `complete` | `0` | Manually re-read MySQL SQL-backed sources, legacy MariaDB class, current envs, and `SuiteMysql`. |
| `postgresql` | `complete` | `complete` | `complete` | `0` | Manually re-read all 8 legacy PostgreSQL SQL-backed sources, current inherited classes, envs, and `SuitePostgresql`. |
| `sql-server` | `complete` | `complete` | `complete` | `0` | Manually re-read legacy/current class, `SqlServerEnvironment`, and `SuiteSqlServer`. |
| `cassandra` | `complete` | `complete` | `complete` | `0` | Manually re-read legacy/current class, `CassandraEnvironment`, and `SuiteCassandra`. |
| `clickhouse` | `complete` | `complete` | `complete` | `0` | Manually re-read legacy/current class, `ClickHouseEnvironment`, and `SuiteClickhouse`; topology difference recorded. |
| `blackhole` | `complete` | `complete` | `complete` | `0` | Manually re-read legacy/current class, `BlackHoleEnvironment`, and `SuiteBlackHole`; aggregate legacy source recorded. |
| `kafka` | `complete` | `complete` | `complete` | `0` | Manually re-read legacy/current Kafka classes, launcher/current environments, and `SuiteKafka`; the restored Confluent/basic, SSL, and SASL/PLAINTEXT suite runs now close the prior breadth gap. |
| `ldap` | `complete` | `complete` | `complete` | `0` | Manually re-read legacy/current LDAP CLI and JDBC bodies, launcher/current environments, and `SuiteLdap`; the restored insecure, referrals, bind-DN, and combined-auth rerun coverage closes the prior breadth gap. |
| `jdbc` | `complete` | `complete` | `complete` | `0` | Manually re-read legacy/current `TestJdbc`, `JdbcBasicEnvironment`, and the JDBC portion of `SuiteClients`; environment simplification recorded as non-gap. |
| `jdbc-oauth2` | `complete` | `complete` | `complete` | `0` | Manually re-read legacy/current OAuth2 JDBC classes, launcher/current environment family, and `SuiteOauth2`; the restored proxy, OIDC, refresh, and OIDC-refresh runs close the prior suite-breadth gap. |
| `jdbc-kerberos` | `complete` | `complete` | `complete` | `0` | Manually re-read legacy/current constrained-delegation JDBC class, legacy aggregate suite source, current environment, and `SuiteJdbcKerberos`. |
| `clients` | `complete` | `complete` | `complete` | `0` | Manually re-read legacy/current CLI and JMX classes, `CliEnvironment`, and the CLI extension of `SuiteClients`; the restored aborted-transaction and rollback flows close the prior CLI fidelity gaps. |
| `tls` | `complete` | `complete` | `complete` | `0` | Manually re-read legacy/current TLS class, extracted TLS suite coverage from legacy `Suite3`, and compared `EnvMultinodeTls` to `TlsEnvironment`. |
| `exasol` | `complete` | `complete` | `complete` | `0` | Manually re-read legacy/current Exasol class, launcher/current environments, and `SuiteExasol`; dedicated environment extraction recorded as non-gap. |
| `loki` | `complete` | `complete` | `complete` | `0` | Manually re-read legacy/current Loki class, launcher/current environments, and `SuiteLoki`; dedicated environment extraction recorded as non-gap. |
| `functions` | `complete` | `complete` | `complete` | `0` | Manually re-read legacy/current Teradata function class, shared-vs-dedicated environment shapes, and `SuiteFunctions`. |
| `tpch` | `complete` | `complete` | `complete` | `0` | Manually re-read legacy/current `SuiteTpch` and the shared-vs-dedicated environment routing for this suite-identity lane. |
| `tpcds` | `complete` | `complete` | `complete` | `0` | Manually re-read legacy/current `SuiteTpcds` and the shared-vs-dedicated environment routing for this suite-identity lane. |
| `ranger` | `complete` | `complete` | `complete` | `0` | Manually re-read legacy/current Ranger class, launcher/current environments, and `SuiteRanger`. |
| `fault-tolerant` | `complete` | `complete` | `complete` | `0` | Manually re-read legacy/current fault-tolerant class and documented the dedicated current environment/suite extraction from prior tag-driven coverage. |
| `all-connectors-smoke` | `complete` | `complete` | `complete` | `0` | Manually re-read legacy/current configured-features smoke class, launcher/current all-connectors environments, and `SuiteAllConnectorsSmoke`. |
| `ignite` | `complete` | `complete` | `complete` | `0` | Manually re-read legacy/current Ignite class, launcher/current environments, and `SuiteIgnite`; approved Ignite upgrade differences recorded. |
| `snowflake` | `complete` | `complete` | `complete` | `0` | Manually re-read legacy/current Snowflake class, launcher/current environments, and `SuiteSnowflake`. |
| `hive` | `complete` | `complete` | `complete` | `0` | Completed the remaining Hive method and environment pass; the restored role, absent-partition, `$partitions`, helper-parity, and legacy-translation methods close the prior Hive fidelity gaps. |
| `parquet` | `complete` | `complete` | `complete` | `0` | Manually re-read legacy/current SQL-backed Parquet methods, `EnvMultinodeParquet` vs `ParquetEnvironment`, and `SuiteParquet`. |
| `hive4` | `complete` | `complete` | `complete` | `0` | Manually re-read `TestHiveOnOrcLegacyDateCompatibility`, compared `EnvMultinodeHive4` to `MultinodeHive4Environment`, compared legacy/current `SuiteHive4`, and recorded the separate current-only environment verification classes. |
| `hudi` | `complete` | `complete` | `complete` | `0` | Manually re-read Hudi Spark interoperability and Hive-to-Hudi redirection methods, compared legacy singlenode Hudi environments to current extracted environments, and compared legacy/current `SuiteHudi`. |
| `iceberg` | `complete` | `complete` | `complete` | `0` | Freshly re-read all 186 Iceberg-lane method entries from legacy/current source, rechecked the six Iceberg environment classes, and rechecked `SuiteIceberg`; the mixed Hive/Iceberg view coverage is now restored. |
| `delta-lake` | `complete` | `complete` | `complete` | `0` | Freshly re-read all 198 Delta Lake lane method entries from legacy/current source, rechecked the four Delta environments, and rechecked `SuiteDeltaLakeOss`; no Delta-specific unresolved fidelity gap remains. |
| `compatibility` | `complete` | `complete` | `complete` | `0` | Manually re-read legacy/current compatibility methods, environments, and `SuiteCompatibility`; restored compatibility-server assertions close the prior Hive view compatibility gap. |
| `gcs` | `complete` | `complete` | `complete` | `0` | Manually re-read the three GCS classes against the legacy base/helper flow, compared `EnvMultinodeGcs` to `GcsEnvironment`, and completed `SuiteGcs`. |
| `azure` | `complete` | `complete` | `complete` | `0` | Manually re-read Azure table-format, ABFS filesystem, and sync-partition methods, compared `EnvMultinodeAzure` to `AzureEnvironment`, and compared legacy/current `SuiteAzure`. |
| `databricks` | `complete` | `complete` | `complete` | `0` | Freshly re-read all 25 Databricks-lane methods from legacy/current source, rechecked the six Databricks environment classes, and compared all five versioned suites; split-class method notes and 173 exclusion routing were corrected in this pass. |

## Suite Progress

| Suite | Owning lane | Suite audit status | Note |
| --- | --- | --- | --- |
| `SuiteAllConnectorsSmoke` | `all-connectors-smoke` | `complete` | Compared against legacy `SuiteAllConnectorsSmoke`; tag-based routing and dedicated environment extraction recorded. |
| `SuiteAuthorization` | `hive` | `complete` | Compared against the authorization run embedded in legacy `Suite3`; current suite isolates the same `Authorization` tag on `HiveKerberosImpersonationEnvironment`. |
| `SuiteAzure` | `azure` | `complete` | Compared directly against legacy `SuiteAzure`; the same three Azure/Hive, Delta, and Iceberg runs remain in `cloud-object-store`. |
| `SuiteBlackHole` | `blackhole` | `complete` | Compared against aggregate legacy blackhole coverage. |
| `SuiteCassandra` | `cassandra` | `complete` | Compared against legacy `SuiteCassandra`. |
| `SuiteClickhouse` | `clickhouse` | `complete` | Compared against legacy `SuiteClickhouse`. |
| `SuiteClients` | `jdbc` | `complete` | Compared against legacy `SuiteClients`; current suite decomposes the old mixed launcher run into separate JDBC and CLI environment runs. |
| `SuiteCompatibility` | `compatibility` | `complete` | Compared against legacy `SuiteCompatibility`; Hive view compatibility now again asserts through both the current and compatibility Trino coordinators. |
| `SuiteDeltaLakeDatabricks133` | `databricks` | `complete` | Compared directly against legacy `SuiteDeltaLakeDatabricks133`; shared Databricks tag routing and the 44 mapped-method inventory were freshly rechecked from suite source. |
| `SuiteDeltaLakeDatabricks143` | `databricks` | `complete` | Compared directly against legacy `SuiteDeltaLakeDatabricks143`; the suite still selects four mapped methods even though the current source comment incorrectly claims zero tests. |
| `SuiteDeltaLakeDatabricks154` | `databricks` | `complete` | Compared directly against legacy `SuiteDeltaLakeDatabricks154`; the suite still selects four mapped methods even though the current source comment incorrectly claims zero tests. |
| `SuiteDeltaLakeDatabricks164` | `databricks` | `complete` | Compared directly against legacy `SuiteDeltaLakeDatabricks164`; the suite still selects four mapped methods even though the current source comment incorrectly claims zero tests. |
| `SuiteDeltaLakeDatabricks173` | `databricks` | `complete` | Compared directly against legacy `SuiteDeltaLakeDatabricks173`; the shared Databricks tag set plus `DeltaLakeExclude173` still yields 38 mapped methods. |
| `SuiteDeltaLakeOss` | `delta-lake` | `complete` | Compared directly against legacy `SuiteDeltaLakeOss`; the four-run Minio, HDFS, redirect, and caching structure was freshly rechecked from current and legacy suite source. |
| `SuiteExasol` | `exasol` | `complete` | Compared against legacy `SuiteExasol`; dedicated environment extraction recorded. |
| `SuiteFaultTolerant` | `fault-tolerant` | `complete` | Compared against legacy tag-driven `FAULT_TOLERANT` coverage; no dedicated legacy launcher suite existed. |
| `SuiteFunctions` | `functions` | `complete` | Compared against legacy `SuiteFunctions`; current suite isolates the same function coverage on dedicated `FunctionsEnvironment`. |
| `SuiteGcs` | `gcs` | `complete` | Compared directly against legacy `SuiteGcs`; the same three GCS tag slices still run on one shared GCS environment family. |
| `SuiteHdfsImpersonation` | `hive` | `complete` | Compared against the impersonation runs in legacy `Suite2` and `Suite3`; current suite keeps the three impersonation environments as explicit JUnit runs. |
| `SuiteHiveAlluxioCaching` | `hive` | `complete` | Compared against legacy Alluxio-caching coverage; current suite isolates the same tag on `MultinodeHiveCachingEnvironment`. |
| `SuiteHiveBasic` | `hive` | `complete` | Compared against the `HDFS_NO_IMPERSONATION` portion of legacy `Suite2`; current suite isolates the same non-impersonation Hive-basic coverage. |
| `SuiteHiveKerberos` | `hive` | `complete` | Compared against the `HIVE_KERBEROS` coverage in legacy `Suite2`; current suite isolates the same Kerberos smoke path. |
| `SuiteHiveSpark` | `hive` | `complete` | Compared against legacy Hive/Spark compatibility coverage; current suite keeps the Spark and no-stats-fallback runs explicitly separated. |
| `SuiteHiveStorageFormats` | `hive` | `complete` | Compared against the broad `STORAGE_FORMATS` coverage previously spread across legacy `Suite2`; current suite isolates the non-detailed, non-HMS-only storage-format slice. |
| `SuiteHiveTransactional` | `hive` | `complete` | Compared against legacy `SuiteHiveTransactional`; current suite keeps the same dedicated transactional lane on the extracted environment. |
| `SuiteHive4` | `hive4` | `complete` | Compared directly against legacy `SuiteHive4`; the same single Hive4 run remains in `hive-kerberos`. |
| `SuiteHmsOnly` | `hive` | `complete` | Compared against legacy `SuiteHmsOnly`; current suite keeps the dedicated HMS-only run on the extracted storage-formats environment. |
| `SuiteHudi` | `hudi` | `complete` | Compared directly against legacy `SuiteHudi`; the same two Hudi and Hive-to-Hudi redirection runs remain in `hive-kerberos`. |
| `SuiteIceberg` | `iceberg` | `complete` | Compared directly against legacy `SuiteIceberg`; the six-run Spark, redirection, REST, JDBC, Nessie, and caching structure was freshly rechecked from current and legacy suite source. |
| `SuiteIgnite` | `ignite` | `complete` | Compared against legacy `SuiteIgnite`; approved Ignite-upgrade environment changes recorded. |
| `SuiteJdbcKerberos` | `jdbc-kerberos` | `complete` | Compared against legacy aggregate `Suite3` constrained-delegation run; current suite isolates that coverage. |
| `SuiteKafka` | `kafka` | `complete` | Manually compared against legacy `SuiteKafka`; current suite again covers the basic, Confluent/basic, SSL, SASL/PLAINTEXT, schema-registry, and Confluent-license runs. |
| `SuiteLdap` | `ldap` | `complete` | Compared against legacy five-environment launcher suite; current JUnit suite again covers the basic, combined-auth, combined-auth rerun, insecure, referrals, and bind-DN runs. |
| `SuiteLoki` | `loki` | `complete` | Compared against legacy `SuiteLoki`; dedicated environment extraction recorded. |
| `SuiteMysql` | `mysql-mariadb` | `complete` | Compared against legacy `SuiteMysql`. |
| `SuiteOauth2` | `jdbc-oauth2` | `complete` | Compared against legacy eight-run launcher suite; current suite again covers direct OAuth2, direct OIDC, both proxy variants, both authenticated proxy variants, OAuth2 refresh, and OIDC refresh. |
| `SuiteParquet` | `parquet` | `complete` | Compared directly against legacy `SuiteParquet`; single-run tag and environment shape preserved. |
| `SuitePostgresql` | `postgresql` | `complete` | Compared against effective legacy `Suite7NonGeneric` PostgreSQL runs. |
| `SuiteRanger` | `ranger` | `complete` | Compared against legacy `SuiteRanger`; dedicated environment extraction recorded. |
| `SuiteSnowflake` | `snowflake` | `complete` | Compared against legacy `SuiteSnowflake`; dedicated environment extraction recorded. |
| `SuiteSqlServer` | `sql-server` | `complete` | Compared against effective legacy `Suite7NonGeneric` SQL Server run. |
| `SuiteStorageFormatsDetailed` | `hive` | `complete` | Compared against legacy `SuiteStorageFormatsDetailed`; current suite keeps the detailed/compression split as two explicit runs on the same environment. |
| `SuiteTls` | `tls` | `complete` | Compared against the TLS run inside legacy `Suite3`; current suite is a dedicated extraction on `TlsEnvironment`. |
| `SuiteTpcds` | `tpcds` | `complete` | Compared against legacy `SuiteTpcds`; current suite keeps the same identity coverage on dedicated `FunctionsEnvironment`. |
| `SuiteTpch` | `tpch` | `complete` | Compared against legacy `SuiteTpch`; current suite keeps the same identity coverage on dedicated `FunctionsEnvironment`. |
| `SuiteTwoHives` | `hive` | `complete` | Compared against legacy `EnvTwoKerberosHives` and `EnvTwoMixedHives` coverage; current suite preserves the two-run structure explicitly. |
