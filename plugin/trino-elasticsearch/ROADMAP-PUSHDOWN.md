# Elasticsearch Connector Pushdown Roadmap

This roadmap tracks the staged implementation of the Elasticsearch connector pushdown architecture. A stage is not considered complete until its implementation-specific tests and connector-level checks pass.

## Delivery rules

1. Work on a feature branch; never implement directly on `master`.
2. Keep each stage reviewable and independently testable.
3. P0.1-P0.5 may be implemented as one integration batch on the P0 feature branch, but P1 work must not start until the complete P0 gate is green.
4. Every behavior change must include regression tests covering exact SQL semantics and generated Elasticsearch behavior.
5. Prefer exact pushdown. Candidate-only pushdown must retain the Trino residual predicate. Approximate pushdown is allowed only behind explicit UNSAFE opt-in and must be marked as approximate in the Remote Predicate IR.
6. Dynamic filters must be exact: join re-checking can remove false positives but cannot recover rows lost to approximate false negatives.
7. Avoid Elasticsearch scripts for generic predicate pushdown unless there is no index-native equivalent and the performance/correctness trade-off is demonstrated.
8. Keep upstream contribution in mind: isolate performance changes from architectural refactors where possible.

## Test gate used for every implementation stage

Run the narrowest unit tests first, then the connector module checks before marking a stage complete:

```bash
./mvnw -pl :trino-elasticsearch -Dtest=<affected-test-class> test
./mvnw -pl :trino-elasticsearch airstyle:check
./mvnw -pl :trino-elasticsearch test
```

For the current P0 integration branch, implementation is completed first and formatting/Maven/CI failures are then fixed as a batch. GitHub CI must be green before P0 is marked complete.

---

## P0.1 — Remote Predicate IR

**Status:** COMPLETE — FULL TEST GATE GREEN

### Objective

Replace the current collection of loosely related remote-predicate state (`TupleDomain`, regex maps, prefix maps, match-phrase-prefix maps and synthetic domains) with a composable remote predicate representation that can express multiple predicates on the same Elasticsearch field without losing semantics.

### Target model

```text
RemotePredicate
├── And
├── Or
├── Not
├── Enforced
│   └── EXACT / PREFILTER / APPROXIMATE metadata
├── Term
├── Terms
├── Range
├── Prefix
├── Regexp
├── MatchPhrase
├── MatchPhrasePrefix
└── Exists
```

Enforcement semantics:

```text
EXACT        remote predicate is authoritative
PREFILTER    remote predicate only reduces candidates; Trino residual is required
APPROXIMATE  remote predicate intentionally uses approximate full-text semantics after UNSAFE opt-in
```

`EXACT` is the default for primitive IR nodes. `PREFILTER` and `APPROXIMATE` are retained explicitly through the `Enforced` IR wrapper and table-handle JSON serialization. The wrapper is transparent to Elasticsearch DSL rendering.

### Implementation sequence

- [x] P0.1a Add immutable/sealed Remote Predicate IR model.
- [x] P0.1b Add Elasticsearch DSL renderer for the IR without changing current query behavior.
- [x] P0.1c Add unit tests for primitive nodes, enforcement metadata and boolean composition.
- [x] P0.1d Add an optional remote predicate field to `ElasticsearchTableHandle`.
- [x] P0.1e Teach `ElasticsearchQueryBuilder` to compose legacy constraints and the new IR.
- [x] P0.1f Add round-trip/table-handle tests and regression tests.
- [x] P0.1g Run full connector test gate and GitHub CI.

### Acceptance criteria

- Multiple remote predicates can target the same field.
- `AND`, `OR`, and `NOT` are representable without map-key collisions.
- Enforcement metadata survives planning and serialization.
- Existing legacy state can be canonicalized into the IR during migration.
- No synthetic `TupleDomain` workaround is required for newly migrated runtime predicates.

---

## P0.2 — Native Elasticsearch `terms`

**Status:** COMPLETE — FULL TEST GATE GREEN

### Objective

Translate discrete multi-value predicates to native Elasticsearch `terms` queries instead of generating a large `bool.should` list of `term` queries.

### Scope

- [x] Single discrete value -> `Term`.
- [x] Multiple discrete values -> `Terms`.
- [x] Numeric values.
- [x] Boolean values.
- [x] Timestamp/date values supported by the connector.
- [x] `keyword` and safe `.keyword` paths for VARCHAR.
- [x] Preserve residuals when exact semantics are not guaranteed.

### Required tests

- [x] `IN` with 1, 10, 1,000 and more than 1,024 values through the ES7/ES8 P0 connector base test.
- [x] Generated DSL contains one native `terms` query instead of >1,024 bool clauses.
- [x] Connector results are checked against reference Trino execution.
- [x] Case-preserving remote field names remain correct.

### Acceptance criteria

Large `IN (...)` predicates no longer depend on Elasticsearch's bool-clause limit for the common discrete-domain case.

---

## P0.3 — Primitive Array Exact Pushdown

**Status:** COMPLETE — FULL TEST GATE GREEN

### Objective

Push only primitive-array predicates whose Trino semantics map cleanly to Elasticsearch multi-valued fields.

### Supported first

#### `contains(array, constant)`

```sql
contains(tags, 'telegram')
```

-> `Term(tags.keyword, 'telegram')` when an exact keyword representation exists.

#### `arrays_overlap(array, constant_array)`

```sql
arrays_overlap(tags, ARRAY['telegram', 'facebook'])
```

-> `Terms(tags.keyword, ['telegram', 'facebook'])`.

### Primitive element types

- [x] TINYINT / SMALLINT / INTEGER / BIGINT
- [x] REAL / DOUBLE
- [x] BOOLEAN
- [x] TIMESTAMP_MILLIS backed by Elasticsearch date mappings
- [x] IP where supported by the connector
- [x] VARCHAR backed by exact `keyword`
- [x] VARCHAR backed by safe `text.keyword`

### Explicitly not exact-pushed

- [x] `array_col = ARRAY[...]`
- [x] `array_col[index] = value`
- [x] `element_at(array_col, n)`
- [x] `array_position(...)`
- [x] `contains_sequence(...)`
- [x] `cardinality(array_col)`
- [x] analyzed-text-only array membership
- [x] whole-array `IS NULL` / `IS NOT NULL` unless semantics are proven equivalent for empty arrays

### Required tests

- [x] ES7 and ES8 connector acceptance suite.
- [x] Empty array vs NULL/missing behavior.
- [x] NULL elements in constant `arrays_overlap` arrays remain residual.
- [x] Duplicate constant values.
- [x] Numeric arrays.
- [x] Boolean and timestamp translator coverage.
- [x] Exact keyword arrays.
- [x] `text.keyword` case-sensitive membership.
- [x] analyzed text fallback/residual.
- [x] whole-array equality remains non-pushdown.
- [x] Source arrays containing NULL elements: connector-level regression coverage.

---

## P0.4 — Dynamic Filter Planner

**Status:** COMPLETE — INTEGRATION GATE GREEN

### Objective

Use the new `Term`/`Terms` IR to make dynamic filtering scale beyond the current fixed domain-compaction behavior while preserving join correctness.

### Planner

```text
DynamicFilter
  ├── single exact value -> Term
  ├── small exact set -> Terms
  ├── medium exact set -> batched Terms
  ├── exact range -> Range
  ├── analyzed/approximate field -> no remote dynamic filter
  └── excessive/unsafe -> bounded fallback
```

### Configuration

```properties
elasticsearch.dynamic-filtering.max-values
elasticsearch.dynamic-filtering.terms-batch-size
elasticsearch.dynamic-filtering.max-query-bytes
```

### Required tests

- [x] Small dynamic-filter value set.
- [x] >1,000 values without losing selectivity merely because of the old hard-coded compaction threshold.
- [x] Batched query generation.
- [x] Request-byte budget.
- [x] Correct fallback when value/query budget is exceeded.
- [x] Analyzed-text dynamic filters are rejected rather than approximated.
- [x] Configuration defaults and explicit mappings.
- [x] Join integration test proving dynamic filtering reaches Elasticsearch and reduces source input.

### Correctness invariant

Dynamic filtering never uses approximate analyzed-text matching. A join can re-check false positives, but it cannot recover a source row that Elasticsearch incorrectly removed as a false negative.

---

## P0.5 — Complete Rule-based Predicate Migration

**Status:** COMPLETE — FULL TEST GATE GREEN

### Objective

Move predicate-specific planning out of the monolithic legacy path and into composable expression/domain translation rules targeting the Remote Predicate IR.

### Rules

- [x] Exact discrete domain
- [x] Range domain
- [x] Exact LIKE/prefix
- [x] analyzed-text LIKE
- [x] `starts_with`
- [x] `substr` / `substring` prefix recognition
- [x] `regexp_like`
- [x] array `contains`
- [x] `arrays_overlap`

### Cleanup after migration

- [x] Runtime P0 rules no longer require synthetic full-text domains; the DomainTranslator-generated analyzed prefix range is removed only when provenance is proven safe.
- [x] New P0 rules target Remote Predicate IR and do not write legacy regex/prefix/match-phrase-prefix maps.
- [x] Legacy maps are retained only as compatibility state while fallback behavior remains; any fallback state is immediately canonicalized into IR.
- [x] Preserve full-text modes DISABLED / SAFE / UNSAFE.
- [x] SAFE keeps Trino residuals and records `PREFILTER`; UNSAFE records `APPROXIMATE`.
- [x] Multiple regexp predicates can target the same field without map collisions.
- [x] Remote predicates are preserved across limit/aggregation handle rewrites.
- [x] Statistics do not silently ignore remote predicates: until statistics become IR-aware, the rule-based facade returns conservative empty statistics for filtered handles.

---

## P0 final gate

**Status:** GREEN

Validated on PR #15 at head `ece12f0dedbaa05b9fee1c50f9659dd03f5f0f69` with GitHub Actions CI run #151 (`32685412482`) completed successfully.

Before P1 starts:

- [x] Add connector-level source-array NULL-element regression coverage.
- [x] Add/verify join integration coverage for dynamic filtering reaching Elasticsearch.
- [x] Run focused P0 unit tests.
- [x] Run `./mvnw -pl :trino-elasticsearch airstyle:check` and fix formatting as one final batch.
- [x] Run `./mvnw -pl :trino-elasticsearch test`.
- [x] Inspect and fix compile/API/test failures rather than treating every Maven failure as formatting.
- [x] GitHub CI for PR #15 is green.

P1 work may now start from this validated P0 baseline.

---

## P1.1 — `any_match` Primitive Array Pushdown

**Status:** NOT STARTED

Implement only lambda forms that have provable existential semantics on an Elasticsearch multi-valued field:

- [ ] `any_match(a, x -> x = constant)` -> `Term`
- [ ] `any_match(a, x -> x IN (...))` -> `Terms`
- [ ] `any_match(a, x -> x >/< />=/<= constant)` -> `Range`
- [ ] combinations whose boolean semantics remain exact

Do not initially support arbitrary lambdas, `all_match`, `none_match`, or script-backed execution.

---

## P1.2 — Multi-predicate Boolean Composition

**Status:** NOT STARTED

### Objective

Fully exploit the IR to allow multiple independent predicates on the same field.

Examples:

```sql
name LIKE '%ngô%' AND name LIKE '%văn%'
```

-> two full-text predicates under remote `AND` when allowed by the configured full-text mode.

```sql
contains(tags, 'a') AND contains(tags, 'b')
```

-> two `Term` predicates under `AND`.

Implement in order:

- [ ] AND
- [ ] OR
- [ ] NOT only after SQL/Elasticsearch NULL semantics are proven safe

---

## P1.3 — Pushdown Observability

**Status:** NOT STARTED

Expose enough information to debug production pushdown decisions:

- [ ] SQL/connector expression -> Remote Predicate IR diagnostic output.
- [ ] Remote Predicate IR -> generated Elasticsearch DSL diagnostic output.
- [ ] EXACT / PREFILTER / APPROXIMATE / residual counts.
- [ ] `terms` query/value counts.
- [ ] array-membership pushdown counts.
- [ ] dynamic-filter values received/pushed/compacted/batched.
- [ ] remote requests, rows, bytes, pages and retries where available.

---

## P1.4 — Scan Execution v2

**Status:** NOT STARTED

### Objective

Reduce heap usage and improve large-scan throughput after predicate planning is stabilized.

- [ ] Benchmark current Scroll execution.
- [ ] Benchmark PIT + `search_after` where supported.
- [ ] Evaluate Jackson streaming parsing instead of String -> JsonNode -> Map materialization.
- [ ] Improve cancellation/clear-scroll resource handling.
- [ ] Add completed-byte and page/request accounting.
- [ ] Keep ES version compatibility explicit.

Do not replace Scroll without benchmark evidence.

---

## P2 — TopN, LIMIT, Aggregation and Statistics Hardening

**Status:** NOT STARTED

- [ ] Review shard-aware LIMIT/TopN over-fetch strategy.
- [ ] Improve early cancellation where safe.
- [ ] Make composite aggregation page size configurable if benchmarks justify it.
- [ ] Add aggregation resource/byte metrics.
- [ ] Make statistics rendering fully Remote-Predicate-IR-aware, then add caching/selectivity improvements only after measuring planning overhead.

---

## P3 — Low-priority SPI Extensions

**Status:** NOT STARTED

### `applySample`

Research only after P0/P1. Elasticsearch sampling semantics must be compared carefully with Trino TABLESAMPLE semantics.

### `applyJoin`

Do not prioritize generic join pushdown. Preferred architecture is:

```text
Trino join
  -> build-side dynamic filter
  -> DynamicFilterPlanner
  -> exact Term/Terms/Range
  -> Elasticsearch
```

---

## Current execution order

```text
P0 COMPLETE / GREEN
  -> P1.1 any_match
  -> P1.2 boolean composition
  -> P1.3 observability
  -> P1.4 scan execution v2
  -> P2 hardening
  -> P3 optional SPI work
```

Update this file after every completed stage, including the tests/CI used to validate completion.
