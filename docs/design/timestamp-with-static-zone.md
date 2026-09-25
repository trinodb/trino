# Timestamp with static zone (`TIMESTAMP(p) WITH TIME ZONE 'Z'`) design

Status: draft, 2026-09-22. Author: Łukasz Osipiuk (with Claude).
Related: [trinodb/trino#2273](https://github.com/trinodb/trino/issues/2273),
Slack thread in `#dev` (Piotr Findeisen, Dain Sundstrom, Martin Traverso, 2026-09-19..22),
branch `lo/static-tz-timestamp`.

---

## 1. Problem

Trino's `TIMESTAMP(p) WITH TIME ZONE` stores a zone in every value. Almost no source
system has a zone per value: Iceberg `timestamptz`, Delta `timestamp`, Hive
`timestamp with local time zone`, PostgreSQL / MySQL / Redshift `timestamptz`,
Cassandra, MongoDB, BigQuery and others all store an instant. Every one of those
connectors attaches `UTC_KEY` on read and silently discards the zone on write.

Because the type does not say "the zone is always UTC", the engine cannot know it.
Consequences, all present in the tree today:

1. **Denser representation is impossible.** `timestamp(6) with time zone` (Iceberg's
   most common type) is a 12-byte `Fixed12Block` with an object Java type
   (`LongTimestampWithTimeZone`), while `timestamp(6)` is a primitive `long`.
   The packed-zone encoding also steals 12 bits, capping epoch millis at 2^51
   (`DateTimeEncoding.isValidMillisUtc`).
2. **Planner rules bail out.** `UnwrapDateTruncInComparison` and
   `UnwrapYearInComparison` return empty for `TimestampWithTimeZoneType` with the
   comment *"unwrapping is possible only when values are all of some fixed zone and the
   zone is known"*. `UnwrapCastInComparison` only unwraps `CAST(ts_tz AS date)` when the
   constant's zone equals the session zone and there is no DST gap.
3. **Connectors re-implement the planner.** `UtcConstraintExtractor`
   (525 lines, `lib/trino-plugin-toolkit`) re-does those three rules assuming UTC, and
   is wired into Iceberg, Delta and base-jdbc `applyFilter`. base-jdbc additionally
   carries `TimestampTimeZoneDomain {ANY, UTC_ONLY}` to decide whether to use it.
4. **Semantics are misleading.** `INSERT` of `TIMESTAMP '... Europe/Warsaw'` into an
   Iceberg `timestamptz` column succeeds and loses the zone with no error, and
   `timezone(col)` always answers `UTC` regardless of what was inserted.

The issue has been open since 2019. The 2020 decision was "map to `TIMESTAMP WITH TIME
ZONE` with UTC for now" (issue comment, 2020-08-15). This doc proposes the type that
decision deferred.

## 2. Goals and non-goals

Goals

- G1. A first-class engine type for "instant, with a zone known at type level".
- G2. Dense runtime representation identical to `TIMESTAMP(p)`: `long` micros for
  p ≤ 6, `LongTimestamp` for p > 6. No zone bits in the value.
- G3. Existing queries, views, clients, and connector DDL keep working when connectors
  switch their instant-typed columns to the new type. Users should not need to change
  SQL; most should not notice.
- G4. Planner can treat the type like `TIMESTAMP(p)` with a fixed zone: unwrap
  `CAST(.. AS date)`, `date_trunc`, `year` in comparisons, pushing ranges to connectors.
- G5. Delete `UtcConstraintExtractor` and `TimestampTimeZoneDomain` once connectors
  migrate.

Non-goals (v1)

- New literal syntax. `TIMESTAMP '2020-01-01 00:00:00 UTC'` stays a
  `timestamp with time zone` literal.
- Native client / JDBC support for the type. v1 always presents it to clients as
  `timestamp(p) with time zone`.
- Changing what `current_timestamp`, `AT TIME ZONE`, `from_unixtime` return at the
  query output. In v1 they keep returning `timestamp with time zone`. The intent is
  for them to return the new type internally once the engine supports it (session
  zone for `current_timestamp`, the argument zone for a constant `AT TIME ZONE`),
  with a cast back to `timestamp with time zone` at the output boundary if needed.
  Whether the query-visible return type changes is a later decision.
- Auto-"downgrading" `timestamp with time zone` columns to the new type based on usage
  (Piotr's original comment). That is a follow-up optimization, and a separate design.

## 3. Options considered

Martin's framing: these are not implementation options, they have materially different
semantics, so pick one by the problem being solved.

| | Semantics | Fits the problem? |
|---|---|---|
| A. `instant` | Point in time, no zone at all. Calendar ops need an external zone (session? error?). | Clean, but either every `extract`/`date_trunc` depends on session zone (legacy `timestamp` semantics the project spent years removing) or is unsupported. Cannot be substituted for today's `timestamptz` columns without changing results of `date_trunc`, `year`, rendering. |
| B. `timestamp with local time zone` (Oracle/Hive/Spark) | Point in time, calendar ops in the **session** zone. | Same session-dependence problem as A. Results of `year(col)` change with `SET TIME ZONE`. Rejected in the 2019 discussion for the same reason. |
| C. `timestamp(p) with static zone Z` | Point in time, calendar ops and rendering in **zone Z, a type parameter**. | Substitutable for existing `timestamptz` columns (Z = UTC gives byte-identical results for every function). Zone is compile-time known so the planner can reason about it. More complex type (zone parameter). |
| D. Keep `timestamp with time zone`, add column-level "zone domain" metadata | No new type. Connector says "all values of this column are UTC" via `ColumnMetadata`/handle. | Only helps `applyFilter`-style optimizations at the scan; the value is still packed, the engine still cannot use a dense block, and the property is lost after the first projection. Does not achieve G2. |

**Recommendation: C.** It is the only option that is a drop-in for what connectors do
today (UTC) while giving both the execution win (G2) and the planner win (G4). Options A
and B are what the zone parameter degenerates to if we ever want them
(`with time zone 'UTC'` behaves as `instant`; a session-zone variant is a
different type and out of scope).

## 4. Semantics (answers to Martin's questions)

### 4.1 Value and type

- `timestamp(p) with static zone Z`, written `timestamp(p) with time zone 'Z'` in SQL
  (§5.3), p ∈ [0, 12], default p = 3 for parity with
  `timestamp with time zone` (both carry the `TODO: should be 6 per SQL spec`).
- Value: an instant with p fractional digits. Range: same as `timestamp(p)`
  (no 2^51 millis limit).
- Z: any zone accepted by `TimeZoneKey` (region ids and fixed offsets). Two types with
  different Z are distinct types. Zone equivalence: `UTC`, `Etc/UTC`, `Z`, `+00:00`
  normalize to one key already (`TimeZoneKey.UTC_EQUIVALENTS`), so they are the same
  type.
- Comparable and orderable. Equality, hashing and ordering compare the instant only.
  This is exactly what `timestamp with time zone` does today
  (`ShortTimestampWithTimeZoneType.equalOperator` uses `unpackMillisUtc` only), so
  joins and `GROUP BY` between the two types agree.

### 4.2 Rendering

`CAST(x AS varchar)`, client output, `format`, `to_iso8601` render the instant in zone
Z using the `timestamp with time zone` format, e.g. `2020-01-01 12:34:56.789 UTC`.
For Z = UTC this is byte-identical to today's output from every connector listed in §1.

### 4.3 Coercions

Implicit coercions (used by function resolution, `UNION`, `CASE`, comparisons, views):

| From | To | Rule |
|---|---|---|
| `static(p, Z)` | `static(p', Z)`, p' ≥ p | precision widening, like `timestamp` |
| `static(p, Z)` | `timestamp(p', Z') with time zone`, p' ≥ p | attach Z. Lossless, instant preserved, order preserving. |
| `static(p, Z1)` | `static(p', Z2)`, Z1 ≠ Z2 | **not coercible**. Common supertype is `timestamp(max(p, p')) with time zone`. |
| `date` | `static(0, Z)` | **not implicit**. Common supertype of `date` and `static(p, Z)` is `timestamp(p) with time zone`; the date side is interpreted in the session zone exactly as today. |
| `timestamp(p)` | `static(p', Z)` | **not implicit**. Common supertype is `timestamp(max(p, p')) with time zone`; the `timestamp` side is interpreted in the session zone exactly as today. |
| `timestamp with time zone` | `static` | **not implicit**. Would make coercion bidirectional and `getCommonSuperType` ambiguous. |

Rule of thumb: a static-zone type only ever widens into (a) the same zone with higher
precision or (b) `timestamp with time zone`. Every other datetime combination meets at
`timestamp with time zone`, so no existing query changes meaning.

Store assignment (`INSERT`, `UPDATE`, `MERGE`, view staleness) is a separate, looser
relation: `TypeCoercion.isStoreAssignable`, used at `StatementAnalyzer:815` and
`:5799`. It gets one extra rule:

- `timestamp(p) with time zone` is store-assignable to `static(p', Z)` for any Z
  (instant preserved, zone dropped, with a cast planted by the planner).

This keeps today's `INSERT INTO iceberg_table SELECT current_timestamp` working, and
documents the "zone is dropped" behaviour in the type system instead of in each
connector's page sink. `timestamp(p)` stays store-assignable through the existing
`timestamp → timestamp with time zone → static` chain, i.e. session-zone
interpretation, same as today.

Explicit `CAST` in both directions for: `varchar`, `char`, `date`, `time(p)`,
`time(p) with time zone`, `timestamp(p)`, `timestamp(p) with time zone`,
`static` ↔ `static` (precision and zone change; zone change is instant-preserving),
`json` (new; `timestamp with time zone` has no JSON cast today, this is a chance to add
both), `variant`.

`CAST(x AS timestamp(p))` from static zone: local date-time in Z. Note that today
`timestamp with time zone → timestamp` uses the value's own zone, not the session
zone, so this is consistent.

### 4.4 Functions and operators

Everything that exists for `timestamp with time zone` is provided, with the value's
zone replaced by Z:

- `year, quarter, month, day, hour, minute, second, millisecond, day_of_week,
  day_of_year, week, year_of_week, last_day_of_month, date_trunc, date_add,
  date_diff, date_format, format_datetime, to_unixtime, to_iso8601, timezone,
  timezone_hour, timezone_minute`, `+`/`-` interval, `-` between two values.
- `date_trunc`, `+ interval` return `static(p, Z)`. `+ interval day to second` promotes
  precision like the tz version.
- `at_timezone(x, zone)` returns `timestamp(p) with time zone` (zone is an expression,
  cannot become a type parameter). `AT TIME ZONE` / `AT LOCAL` desugar to it.
- `with_timezone` is not defined for the type (it takes `timestamp`).
- `EXTRACT(TIMEZONE_HOUR ...)` works and is a constant per type.
- Aggregations and generic operators (`min`, `max`, `array_agg`, `approx_distinct`,
  `histogram`, `IN`, `BETWEEN`) come for free via `TypeOperators` and the implicit
  coercion to `timestamp with time zone` for any function that only has tz overloads.
  The coercion costs a pack per row, which is why native overloads for the hot
  functions above matter.

Nothing depends on the session zone. That is the property that makes G4 possible.

### 4.5 Literals and constants

No literal syntax. Constants arise from `CAST`, constant folding, and connector column
values. `LiteralInterpreter` / `ExpressionAnalyzer` need cases only for `CAST` of a
string constant. `TIMESTAMP '...'` with a zone remains `timestamp with time zone` and
compares with the new type via coercion, as it does with any tz column today.

## 5. Engine design

### 5.1 SPI type

`io.trino.spi.type.TimestampWithStaticZoneType` (abstract sealed) with
`ShortTimestampWithStaticZoneType` (p ≤ 6, `long` epoch micros, `LongArrayBlock`) and
`LongTimestampWithStaticZoneType` (p > 6, `LongTimestamp`, `Fixed12Block`). Operators
copied from `ShortTimestampType` / `LongTimestampType`. `getObjectValue` returns
`SqlTimestampWithTimeZone` with zone Z so all existing serializers work unchanged.

Instances are cached per (p, Z): 13 precisions × ~2000 zone keys is too many to
pre-build, so use a `ConcurrentHashMap` keyed by `(precision, TimeZoneKey)` in the
factory (`createTimestampWithStaticZoneType(int, TimeZoneKey)`), plus pre-built UTC
constants `TIMESTAMP_UTC_SECONDS/MILLIS/MICROS/NANOS/PICOS` since UTC is the 99% case.

### 5.2 Type descriptor and the zone parameter

`TypeDescriptor` supports only `TypeParameter.Numeric` and `TypeParameter.Type`
(`ParameterKind` is dead code). Options for carrying Z:

| Option | Descriptor text | Pros | Cons |
|---|---|---|---|
| Z1. Numeric `TimeZoneKey.getKey()` | `timestamp(6) with time zone 0`? | Zero SPI change to `TypeParameter`. | Unreadable in `typeof`, views, `information_schema`. Key ids are internal (`zone-index.properties`). |
| Z2. New `TypeParameter.Text(String)` record | `timestamp(6) with time zone 'UTC'` | Readable, canonical. | SPI addition; exhaustive `switch`es over the sealed interface must be updated (compiler finds them). Not visible to clients in v1 (§5.6). |
| Z3. No parameter, UTC only | `timestamp(6) with utc zone`? | Simplest, covers every connector in §1. | Locks the name and semantics to one zone; `current_timestamp` etc. can never use it. |

**Decision: Z2.** The zone is a property of the type, the descriptor is what
views persist (`TypeId` is the descriptor's SQL text, `TypeRegistry.getType(TypeId)`
parses it with the SQL grammar), so it must be readable and parseable. Z3 is rejected
because the stated end goal includes `current_timestamp` (session zone) and
`AT TIME ZONE` producing the type.

Consequences of Z2:

- `TypeParameter` gains `record Text(String value)`; `TypeDescriptor.formatValue` and
  `jsonValue` render it single-quoted (zone ids contain no quotes, but escape anyway).
  Every exhaustive `switch` over the sealed interface (`TypeDescriptorTranslator`,
  `ProtocolUtil.toClientTypeSignatureParameter`, `SignatureBinder`, `TypeTemplates`)
  gets a case. The client protocol case is unreachable in v1 (§5.6) and throws.
- Function signatures need a **zone variable** so that `date_trunc`, `+ interval`,
  `CAST` between precisions and similar can declare "returns the same zone as the
  input": `@SqlType("timestamp(p) with time zone z")` binding `z`. `TypeTemplate` /
  `TemplateParameter` today have type variables and numeric variables; add a text
  variable kind, bound by `SignatureBinder` from the argument's `Text` parameter. The
  template grammar accepts an identifier where a type expects the zone string.
- The zone in the descriptor is the canonical `TimeZoneKey` id, normalized at type
  construction, so descriptor equality is zone equality.

### 5.3 Syntax

`TypeId` round-trips through the SQL parser, so the type needs a grammar rule
regardless of whether users are expected to type it. Reuse the existing
`timestamp with time zone` rule in `SqlBase.g4 type` and allow an optional zone
designator at the end:

```
| base=TIMESTAMP ('(' precision=typeParameter ')')? WITH TIME ZONE string?   #dateTimeType
```

`timestamp(6) with time zone` stays the existing per-value-zone type;
`timestamp(6) with time zone 'UTC'` is the static-zone type. No new keyword. Extend
`DateTimeDataType` with `Optional<String> timeZone`.
`TypeDescriptorTranslator.toTypeTemplate/toDataType`, `TypeDescriptor.formatValue`
(already special-cases the datetime names) and `ExpressionFormatter` get matching
cases. Display name: `timestamp(6) with time zone 'UTC'`.

The zone string is validated at analysis time with `TimeZoneKey.getTimeZoneKey` and
normalized to the key's canonical id in the descriptor, so
`timestamp with time zone 'Etc/UTC'` and `timestamp with time zone 'UTC'` are the same
type.

### 5.4 Coercion and resolution

`TypeCoercion`:

- `compatibility()` today finds a common supertype only when one side coerces into
  the other's base. Add one rule ahead of the base-name path: if either side is a
  static-zone type and the pair is not "same zone, differing precision", replace the
  static side(s) by `timestamp(p) with time zone` and recurse. This single rule yields
  the whole §4.3 table: different zones, `date`, `timestamp` and
  `timestamp with time zone` all meet at `timestamp with time zone`, with today's
  session-zone interpretation of the non-static side.
- `compatibility()` same-base branch: same Z → widen precision.
- `coerceTypeBase`: `static → timestamp with time zone` (keep p). No `date → static`
  or `timestamp → static` entries.
- `isStoreAssignable`: add `timestamp with time zone → static`.
- `isInjectiveCoercion`: hard-codes `result instanceof TimestampWithTimeZoneType →
  false`; `static → tz` is injective and should say so.

`DomainTranslator.isOrderPreserving` uses `canCoerce`, so `static → tz` is treated as
order preserving. It is (instant preserved), fine.

### 5.5 Planner

Change the three bail-outs to accept the new type and use Z instead of
`session.getTimeZoneKey()`:

- `UnwrapCastInComparison.isInjectiveOrderPreservingCastAtValue` (`:779`): for
  `static(Z)` target with `date`/`timestamp` source, drop the "constant zone must equal
  session zone" check; keep the DST-gap check against Z.
- `UnwrapDateTruncInComparison` (`:200`), `UnwrapYearInComparison` (`:205`): compute
  period boundaries in Z, as `UtcConstraintExtractor` does today with UTC.
- `UnwrapAtTimeZoneInComparison`: `at_timezone(static, c)` returns tz; extend the
  instant-preserving check to accept a static argument.
- `CanonicalizeExpressionRewriter` `date(x) → CAST(x AS date)`: add the type.
- `RemoveRedundantDateAdd`: add the type.

Once Iceberg, Delta and base-jdbc columns are static-zone, `UtcConstraintExtractor`
receives no tz expressions and can be deleted along with `TimestampTimeZoneDomain`
(G5). Keep it one release for tables served by old connector versions.

### 5.6 Client protocol

Clients have a closed `ClientStandardTypes` set and a 4-kind
`ClientTypeSignatureParameter`. v1 never sends the new type:

- `ProtocolUtil.formatType` / `toClientTypeSignature`: render `static(p, Z)` as
  `timestamp(p) with time zone` (and as bare `timestamp with time zone` when
  `PARAMETRIC_DATETIME` is absent, same as today).
- `JsonEncodingUtils`: `getObjectValue` already yields `SqlTimestampWithTimeZone(Z)`,
  so value encoding and the `roundTo(3)` downgrade need no change.
- `information_schema.columns`, `SHOW COLUMNS`, `DESCRIBE`, `SHOW CREATE TABLE`,
  JDBC `DatabaseMetaData.getColumns` (`ColumnJdbcTable`), `DESCRIBE OUTPUT`,
  `typeof()`: these are varchar and report the real type,
  `timestamp(6) with time zone 'UTC'`. This is the one place the type is user-visible
  in v1. A config property (working name
  `sql.legacy-timestamp-with-time-zone-in-metadata`, default `false`) masks static-zone
  column types as `timestamp(p) with time zone` in those metadata surfaces, for
  deployments whose external tools parse type-name strings (dbt-trino, Superset,
  Tableau). `SHOW CREATE TABLE` round-trips either way, since `getSupportedType` maps
  `timestamp with time zone` back to the static type on Iceberg-style connectors.
  `typeof()` and `EXPLAIN` are never masked.
- Later: `ClientCapabilities.STATIC_ZONE_TIMESTAMP` to send the real type.

### 5.7 Views

`ViewColumn.type` is the descriptor text. Existing views store
`timestamp(p) with time zone`; when the underlying Iceberg table now produces
`static(p, UTC)`, `checkViewStaleness` uses `isStoreAssignable` (true) and
`RelationPlanner:326` plants the cast. Existing views keep working with unchanged
output types.

New views created over the new type persist `timestamp(6) with time zone 'UTC'`.
An older Trino cannot parse that (`INVALID_VIEW`). To keep rollback possible, add a
config property (working name `sql.legacy-timestamp-with-time-zone-in-views`, default
`false`) that makes `CREATE [MATERIALIZED] VIEW` persist the coerced
`timestamp(p) with time zone` type for static-zone output columns. Reading such a view
plants the same cast as for pre-existing views. Operators set it to `true` before
upgrading to the release that migrates connectors and drop it once rollback is no
longer a concern.

## 6. Connector plan

Phase per connector, each behind a catalog config flag for one release
(`<connector>.timestamp-with-time-zone-mapping=static-zone|legacy`, default
`static-zone` after bake time):

| Connector | Today | New | Notes |
|---|---|---|---|
| Iceberg | `timestamptz` → tz(6), `timestamptz_ns` → tz(9) | static(6, UTC) / static(9, UTC) | `getSupportedType`: tz(p) → static(6/9, UTC); `PartitionTransforms` already compute in UTC; `Timestamps.java` helpers become trivial; Parquet/ORC readers emit `long` micros directly (the `Fixed12` → `long` win). `UtcConstraintExtractor` call removed. |
| Delta | `timestamp` → tz(3) | static(3, UTC) | `DeltaLakeWriter` coercer simplifies; `validatePrimitiveType` accepts static(3, UTC). Note Delta hard-rejects other precisions rather than coercing; keep that. |
| Hive | `TIMESTAMPLOCALTZ` → tz(p from `hive.timestamp-precision`) | static(p, UTC) | read-only today; stays read-only. |
| PostgreSQL, MySQL, Redshift, SQL Server(`datetimeoffset` excluded), others in base-jdbc | tz(p) with UTC | static(p, UTC) | `TimestampTimeZoneDomain` removed. SQL Server `datetimeoffset` keeps tz (real per-value zone). |
| Cassandra, MongoDB, BigQuery, Hudi, Loki, JMX, Faker | tz with UTC | static(·, UTC) | mechanical. Faker is genuinely zone-parameterized and can stay tz. |
| `$file_modified_time` (Hive, Iceberg, Delta, Hudi) | tz(3) UTC | static(3, UTC) | Hive's partition-key variant uses the JVM default zone (`HiveUtil:663`), a latent bug this fixes. |

Shared readers: `lib/trino-parquet` `ValueDecoders`/`ColumnReaderFactory`/
`TimestampTz*ValueWriter`, `lib/trino-orc` `TimestampColumnReader`/
`TupleDomainOrcPredicate`, `StatsUtil.toStatsRepresentation`.

`MetadataManager.getSupportedType` enforces `isCompatible(newType, requested)`. tz(p)
and static(p, UTC) have a common supertype (tz), so returning the static type from
`getSupportedType` passes. `CREATE TABLE t (c timestamp(6) with time zone)` on Iceberg
therefore silently creates a `timestamptz` column typed static(6, UTC), matching
Iceberg's own model.

## 7. Rollout

1. **Engine type, hidden.** SPI type, grammar, coercions, casts, function family,
   protocol downgrade, tests. No producer. Ships dark; only reachable via `CAST`.
2. **Planner rules.** Unwrap rules accept the type. Unit tests mirror
   `TestUtcConstraintExtractor` cases in `TestUnwrapCastInComparison` etc.
3. **Connectors, flagged.** Iceberg first (largest win, has `getSupportedType`), then
   Delta, base-jdbc family, the rest. Product tests + `BaseConnectorTest` type-mapping
   tests updated to expect the new type name.
4. **Cleanup.** Delete `UtcConstraintExtractor`, `TimestampTimeZoneDomain`, legacy
   flags. Document the type in `docs/.../language/types.md`.
5. **Later.** Client capability; `current_timestamp`/`AT TIME ZONE` returning the type;
   usage-based downgrade of tz columns.

## 8. Open questions

- **Q1. Name and syntax.** Resolved: reuse `WITH TIME ZONE` with an optional zone
  string, `timestamp(6) with time zone 'UTC'` (§5.3). No new keyword.
- **Q2. Zone parameter encoding.** Resolved: new `TypeParameter.Text`, plus a text
  template variable for function signatures (§5.2).
- **Q3. `date → static` implicit coercion.** Resolved: no implicit coercion; `date`,
  `timestamp` and differently-zoned static types all meet at
  `timestamp with time zone`, preserving today's session-zone semantics (§4.3, §5.4).
- **Q4. Visibility in metadata.** Resolved: report the real type, with a config
  property to mask it as `timestamp with time zone` in metadata surfaces (§5.6).
- **Q5. View persistence and rollback.** Resolved: a config property makes new views
  persist the coerced `timestamp with time zone` type (§5.7).
- **Q6. Precision split.** Resolved: p ≤ 6 as `long`, like `timestamp`.
- **Q7. `timestamp → static` implicit coercion.** Resolved: not implicit (§4.3).
- **Q8. Native overloads.** Resolved: the whole §4.4 family gets native overloads in
  v1, since the pack-per-row coercion would negate the execution win on exactly the
  functions the planner unwraps.

## 9. Test plan (summary)

- `AbstractTestType` subclasses for short/long variants (mirror
  `TestShortTimestampWithTimeZoneType`, including the range bounds now being those of
  `timestamp`).
- `TestTypeCoercion`, `TestTypeRegistry`, `TestTypeDescriptor`, `TestTypeParser`,
  `TestSqlParser` for syntax and coercion tables in §4.3.
- `TestTimestampWithStaticZone` mirroring `TestTimestampWithTimeZone` for every
  function in §4.4, for Z ∈ {UTC, Europe/Warsaw, America/Los_Angeles, +05:30}.
- Planner: `TestUnwrapCastInComparison`, `TestUnwrapDateTruncInComparison`,
  `TestUnwrapYearInComparison` gain static-zone cases ported from
  `TestUtcConstraintExtractor`.
- Protocol: `TestJsonEncodingUtils`, `TestQueryResultsSerialization`, JDBC
  `TestTrinoDatabaseMetaData`.
- Views: `TestMaterializedViews`-style test: view created over tz column, table switched
  to static, view output type unchanged.
- Connector: Iceberg/Delta/JDBC `BaseConnectorTest` type-mapping and predicate pushdown
  tests (`testTimestampWithTimeZonePushdown` variants), verifying the plan pushes the
  same ranges `UtcConstraintExtractor` produced.
