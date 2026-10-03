# Connector review and upstream handoff

This change is delivered to `ydb-platform/ydb-java-dialects`, not to Trino.
The standalone module remains pinned to Trino 483. The upstream comparison
and prototype used Trino `484-SNAPSHOT` at
`8d58343762f56a7d0f13b799f5521ddca6fbcfff`, with Java 25.
Binary or source compatibility between these versions is not assumed.

## Comparison with the pinned Trino tree

| Area | Trino reference | YDB decision |
| --- | --- | --- |
| Packaging | `plugin/trino-postgresql/pom.xml`, root POM, server assembly | An upstream module needs parent-managed `trino-plugin` packaging, root module and server assembly registration, provided SPI dependencies, and generated plugin services. The standalone shaded-driver POM is not an upstream module. |
| SPI | `lib/trino-plugin-toolkit/.../Versions.java` and JDBC sink interfaces | Enforce the compiled SPI version. The 484 provider/sink memory-context signatures differ from 483 and require an explicit port. |
| JOIN | PostgreSQL, MySQL and MariaDB JDBC clients; `DefaultQueryBuilder` | Use framework join plumbing, but a YQL-specific scalar allowlist and key normalization. Do not inherit another database's comparison semantics. |
| Expressions | `ConnectorExpressionRewriter`, JDBC rewrite rules | Preserve parameter occurrence order. Leave overflow-sensitive arithmetic, Unicode trim and unsupported native coercions in Trino. |
| Types | `StandardColumnMappings`, pinned YDB JDBC `MappingGetters`/`YdbTypes` | Use native unsigned and temporal mappings. Uint64 becomes decimal(20,0), not a signed bigint. Decimal writes carry precision and scale. |
| Metadata | `BaseJdbcClient.getColumns` and YDB JDBC metadata | YDB reports neither TABLE_CAT nor TABLE_SCHEM. The virtual `default` schema must not become a metadata row filter or a physical directory. Staging uses the driver's actual identity. |
| Writes | `JdbcPageSink`, `JdbcMergeSink`, `BaseJdbcClient.beginMerge` | Retain ordinary JDBC INSERT plumbing with YDB schema-copy DDL. Row-level writes explicitly require non-transactional mode and use bounded native-typed batches, rather than operation-specific sinks with lost native metadata. |
| Counts | `QueryBuilder.prepareUpdateQuery`/`prepareDeleteQuery` | Pushdown UPDATE/DELETE uses YQL RETURNING, not a preceding COUNT. Row-level counts follow Trino's processed-row contract in the explicitly non-transactional mode. |
| Tests | `BaseConnectorTest`, `BaseConnectorSmokeTest` | Keep inherited capability tests. Add a production-client native JOIN matrix and write tests; the test-only hidden-key client does not establish production CREATE/INSERT semantics. |
| Documentation/CI | `.github/CONTRIBUTING.md`, `.github/DEVELOPMENT.md`, Trino connector docs and CI | An upstream submission also requires documentation registration, formatting, dependency checks, fresh Error Prone, the ordinary CI matrix and CLA. Standalone Maven success does not establish these. |

## Correctness changes

- Equality JOIN covers Bool, signed/unsigned integers, floating point, text,
  bytes and temporal native types, with INNER/LEFT/RIGHT/FULL test cases.
  Uint64 comparisons retain the full range. NaN and signed zero are normalized
  for ordinary equality; null-safe equality, native Decimal and unsafe
  expressions remain in Trino.
- NULLIF and Unicode strpos preserve repeated bind parameters and SQL NULL.
  Integral overflow is not silently converted into YQL NULL or wrapping.
- Native numeric and temporal writers preserve type, range and precision.
  Floating Top-N uses typed NANVL arguments and explicit NULL/NaN ordering.
- INSERT staging copies native columns without confusing Trino's virtual
  schema with JDBC metadata. A separate Serial key avoids reusing the target
  key as staging row identity.
- Physical-key updates fail explicitly before DML. MERGE retains key order,
  handles nullable composite keys and closes each owned batch transaction.
  Failed batches roll back without replay; previous batches can remain.
- Docker unavailability is an integration-test failure, not a silent skip.
  Shared YDB helper use is serialized. CTAS cleanup assertions subscribe to
  the actual rollback completion instead of racing asynchronous cleanup.

## Upstream prototype evidence and limitations

The mistakenly opened `trinodb/trino#31384` is closed. Its CI reported
113 successful checks, 5 failures and 2 cancellations. Failures included
connector tests, fresh Error Prone, commit-message formatting, CLA, and the
aggregate gate. It is not evidence of upstream readiness.

That Linux run did exercise real YDB: the native JOIN class had 25 test
entries, 1 failure and 1 error. The failing cases exposed an overflow assertion
and untyped floating NANVL argument, both addressed in the standalone tests
and implementation. INSERT metadata identity and MERGE failures also required
changes; the old integration result must not be reused as a pass for this tree.

An independent review of prototype commit
`044a7a0646a2271623c854618482f2c94f972dcc` rejected its atomic-MERGE claim:
one writer was not actually enforced, cancellation did not abort the sink,
and staging metadata still used an inconsistent identity.

The separate local Trino checkout preserves follow-up commits:

- `cf6e4cf33f`: abort unfinished MERGE sinks on operator close;
- `bd9a654ce3`: correct INSERT staging metadata identity;
- `dcb449c1f3`: enforce connector limits for unpartitioned MERGE writers.
- `c70d350cf6d2e98d37846f82ff6052d2e780a3c2`: synchronize the reviewed scalar
  and write semantics, adapt the 484 sink-provider API, and fix fresh Error
  Prone findings. Both implementations now require explicitly
  non-transactional MERGE; the engine fixes are preserved separately, not
  treated as prerequisites for this batch contract.
- `81f4a2b47eb50396f5176cca15a1ed3b215b39d1`: correct modulo and extended
  timestamp boundaries found by the independent gate; fresh source archives
  and all 21 local unit/API tests were rebuilt successfully.
- `c4041e10d8`: the intermediate SQL CAST solution passed unit tests but the
  added live NOT NULL write exposed YQL's Optional result type.
- `fe21afac93f670cc545aeb9b0f25644b62053d92`: use the driver's native SDK-value
  path instead. The inclusive endpoint is encoded with the Timestamp64 type,
  not an optional CAST. Tests now exercise real driver required/optional value
  conversion, not only recorded setter arguments; all 21 unit/API tests pass.
- `dce9fe0f112e2341d645ed8282dbc61173e78864`: retain scalar parameters for
  `IN`, avoiding JDBC 2.4.1's list rewrite that bypasses native SDK values.
  Pushdown stays enabled and the live test covers both Timestamp64 endpoints.
  Fresh Error Prone, packaging and all 21 unit/API tests pass.

Its affected core planner/operator group passed 21 tests. Those engine changes
are not part of Trino 483 and are not silently assumed here. The current
standalone implementation deliberately makes only a non-transactional MERGE
claim. Statement-atomic MERGE needs additional design and verification.

The local Trino checkout is
`/Users/kurdyukov-kir/IdeaProjects/ydb-upstream-work/trino`, branch
`add-ydb-connector`. The upstream submission belongs to the student's handoff;
no replacement upstream PR is created by this work.

## Validation

With Temurin 25.0.2, the following standalone command passed 24 tests, with
zero failures, errors or skips, and built the module:

```bash
mvn -B -ntp -f ydb-trino-adapter/pom.xml \
  -Dtest=TestYdbExpressionRewrites,TestYdbColumnMappings,TestYdbJoinMappings,TestYdbMergeSink,TestYdbWriteMetadata,TestYdbPlugin \
  verify
```

The local run used a separate Maven repository via `-Dmaven.repo.local`.
Java LSP diagnostics were unavailable because jdtls is not installed.
Docker's Colima socket was unavailable; Docker/Colima was neither restarted
nor repaired. The local full `clean test` attempted 22 entries: 18 passed,
4 integration classes failed in helper setup, with zero errors or skips.
No local integration test completed. The ordinary module CI runs:

```bash
mvn -B -ntp -f ydb-trino-adapter/pom.xml clean test
```

Canonical PR 270's first real-YDB run (`36731448784`) reported 354 tests,
4 failures, 1 error and 85 skips. The scalar JOIN matrix and smoke suite ran.
It exposed NaN ordering dependent on both sort direction and NULL placement,
overly broad Date32 predicate fallback, a cached JDBC context-close race, and
two H2 reference queries using unsupported typed-literal syntax.
The follow-up checks NaN ordering against the actual Trino type operator,
pushes only safely bounded Date32 predicates, defaults to uncached JDBC
contexts, and corrects the H2 syntax.

The follow-up [real-YDB CI run 36734257262](https://github.com/ydb-platform/ydb-java-dialects/actions/runs/36734257262)
passed on `2cd8bbede0242ef1a08844e8477f9f5ca02d47e1`: **359 tests,
274 passed, 85 skipped, zero failures or errors**. Class totals are:

| Group | Tests | Skipped |
| --- | ---: | ---: |
| Inherited connector contract | 273 | 81 |
| Inherited smoke contract | 35 | 4 |
| Production native JOIN matrix | 25 | 0 |
| Production CREATE/INSERT/UPDATE/DELETE/MERGE | 6 | 0 |
| Unit and public plugin-bootstrap tests | 20 | 0 |

The synchronized local Trino 484 prototype passed a fresh build, style checks,
Error Prone compilation and 21 unit/API tests:

```bash
./mvnw -B -ntp -pl plugin/trino-ydb -P errorprone-compiler \
  -Dtest=TestYdbClient,TestYdbColumnMappings,TestYdbMergeSink,TestYdbPushdownSemantics,TestYdbJoinCondition,TestYdbPlugin \
  clean verify
```

The build retains compiler deprecation warnings for the framework's legacy
JDBC JOIN API and an advisory text-block warning in an existing smoke test;
none are suppressed. Docker-backed tests on Trino 484 and its full upstream
CI matrix remain unverified for this local commit. The green standalone
Trino 483 run does not replace those checks. Upstream CLA and publication
are also left to the designated author.

The independent gate of `f90737f` / `c70d350cf6` then found two additional
semantic boundaries not covered by those green runs: signed-minimum modulo
`-1` produces YQL NULL instead of Trino zero, and Timestamp64 predicates
could bind values outside the native range. Both implementations now keep
modulo by `-1` in Trino and guard Timestamp64 domains, expression constants,
and writes. Regressions cover filter retention, projection/JOIN results,
native endpoints, just-outside bounds, NULL and unbounded predicates.
The subsequent exact-head CI and delta review must cover these additions.

The current PR's exact-head CI and independent review, not the historical
prototype or earlier PRs, determine whether this branch is ready to merge.
