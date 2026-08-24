# Elasticsearch Connector Pushdown Roadmap

This roadmap tracks the staged implementation of the Elasticsearch connector pushdown architecture. A stage is not considered complete until its implementation-specific tests and connector-level checks pass.

The roadmap is organized around permanent architectural foundations. Later phases may extend coverage, add strategies, or optimize implementations, but must not invalidate or replace the architectural contract established by an earlier phase.

## Delivery rules

1. Work on a feature branch; never implement directly on `master`.
2. Keep each stage reviewable and independently testable.
3. Every behavior change must include regression tests covering exact SQL semantics and generated Elasticsearch behavior.
4. Prefer exact pushdown. Candidate-only pushdown must retain the Trino residual predicate. Approximate pushdown is allowed only behind explicit UNSAFE opt-in and must be marked as approximate in the Remote Predicate IR.
5. Dynamic filters must be exact: join re-checking can remove false positives but cannot recover rows lost to approximate false negatives.
6. Avoid Elasticsearch scripts for generic predicate pushdown unless there is no index-native equivalent and the performance/correctness trade-off is demonstrated.
7. Keep upstream contribution in mind, but fork development correctness and architectural continuity take precedence over minimizing PR scope.
8. **No throwaway phase architecture.** Every roadmap phase must implement against the intended long-lived architecture. A phase must not introduce a temporary representation, API, execution path, planner abstraction, or compatibility mechanism with the expectation that a later phase will replace it. Later phases may extend, specialize, optimize, or generalize an earlier design, but must not invalidate or supersede its architectural foundation.
9. If a planned later capability reveals that the current architecture cannot support it cleanly, stop implementation and revise the roadmap and architecture first. Fix or generalize the current foundation before marking the phase complete; do not knowingly ship an intermediate design solely to unblock the next phase.
10. Compatibility bridges are allowed only when required to coexist with legacy state or an external interface that cannot be migrated atomically. Such bridges are migration boundaries, not target architecture: new roadmap phases must not build new functionality on top of a bridge that is already planned for removal.
11. Before implementation of each phase, the planner must verify architectural continuity: identify the permanent abstractions the phase uses, show how subsequent known phases extend them, and explicitly reject any design whose planned lifecycle is “implement now, replace later.”
12. A phase is not complete merely because CI is green. Its architectural invariants, SQL semantics, Elasticsearch semantics, residual behavior, failure behavior, and resource lifecycle must be explicitly tested.

## Permanent architectural foundations

These abstractions are the long-lived architecture for all remaining phases.

### 1. Remote Predicate IR is the canonical remote filter representation

```text
Connector predicate sources
├── TupleDomain / Domain
├── ConnectorExpression
├── dynamic filters
└── future predicate-producing planner features
             │
             ▼
Predicate translation rules
             │
             ▼
Remote Predicate IR
├── And
├── Or
├── Not
├── Enforced
│   └── EXACT / PREFILTER / APPROXIMATE
├── Term
├── Terms
├── Range
├── Prefix
├── Regexp
├── MatchPhrase
├── MatchPhrasePrefix
└── Exists
             │
             ▼
Elasticsearch DSL renderer
```

New predicate functionality must target this IR. No new parallel regex/prefix/domain maps or synthetic-domain transport mechanisms may be introduced.

### 2. Translation result separates remote work from residual correctness

Predicate translation must preserve, as one semantic result:

```text
Translation result
├── remote predicate, when available
├── exactness / enforcement
├── residual requirement
└── reason/capability metadata needed for diagnostics
```

The exact Java shape may evolve while the phase is being designed, but once P1.2 starts implementation the chosen translation-result contract is permanent. Later phases consume or extend it; they must not replace it with a second planner model.

### 3. Boolean composition is semantic, not syntactic

Top-level SQL boolean composition and array-element lambda composition are different semantic scopes.

```text
Document scope
contains(tags, 'a') AND contains(tags, 'b')
    -> two independent Term predicates may match different array elements

Same-element scope
any_match(values, x -> x > 10 AND x < 20)
    -> both conditions must hold for the same logical array element
```

The planner must never flatten these scopes into a representation that changes semantics. `ElasticsearchArrayPredicateTranslator` remains responsible for proving same-element safety before lowering an `any_match` predicate to the general Remote Predicate IR.

### 4. Table-handle remote predicate state remains authoritative

`ElasticsearchTableHandle` remote predicate state is the durable planner-to-execution contract. Limit, TopN, aggregation, projection, statistics, and scan planning must preserve or deliberately consume that state; none may silently ignore it.

### 5. Execution strategy is a stable abstraction before alternative pagination is added

Scroll, PIT + `search_after`, and any future search execution mode must be implementations of one stable search-execution lifecycle rather than successive replacements.

```text
Search execution contract
├── open
├── request next page
├── decode hits
├── account rows/bytes/requests
├── cancellation
├── retry/failure classification
└── close resources

Implementations
├── Scroll strategy
└── PIT + search_after strategy
```

The existing Scroll path becomes the first implementation of the permanent strategy contract. PIT is added as another implementation only after benchmark and compatibility evidence.

### 6. Observability is emitted from permanent planner/execution models

Do not add temporary log-only instrumentation that later has to be replaced by metrics. P1.3 establishes one diagnostic/event model consumed by logs, metrics, tests, and future troubleshooting surfaces.

## Architectural continuity matrix

| Permanent foundation | Established/strengthened in | Extended by later phases |
| --- | --- | --- |
| Remote Predicate IR | P0 | P1.2, P1.3, P2 statistics |
| Predicate translation result and residual contract | P1.2 | P1.3 diagnostics, future predicate rules |
| Same-element array semantic boundary | P1.1/P1.2 | future exact array-lambda coverage |
| ElasticsearchTableHandle remote planning state | P0 | P1.2, P2 |
| Pushdown diagnostics/event model | P1.3 | P1.4 execution metrics, P2 aggregation/statistics metrics |
| Search execution strategy/lifecycle | P1.4 | P2 LIMIT/TopN early termination and future execution strategies |
| Common resource/accounting contract | P1.4 | P2 aggregation and statistics execution |

If a proposed implementation cannot fit this matrix without replacing a foundation, update the architecture first and do not begin coding.

## Test gate used for every implementation stage

Run the narrowest unit tests first, then the connector module checks before marking a stage complete:

```bash
./mvnw -pl :trino-elasticsearch -Dtest=<affected-test-class> test
./mvnw -pl :trino-elasticsearch airstyle:check
./mvnw -pl :trino-elasticsearch test
```

GitHub CI must be green before a phase is marked complete. CI failures must be traced to their actual source; formatting, compile, planner, semantic, integration, and runtime failures are not interchangeable.

---

# P0 — Predicate Pushdown Foundation

**Status:** COMPLETE — GREEN

P0 established the permanent Remote Predicate IR, native `terms`, primitive-array exact pushdown, exact dynamic-filter planning, and rule-based predicate migration.

## P0.1 — Remote Predicate IR

**Status:** COMPLETE — FULL TEST GATE GREEN

Completed capabilities:

- [x] Immutable/sealed Remote Predicate IR.
- [x] Elasticsearch DSL renderer.
- [x] EXACT / PREFILTER / APPROXIMATE enforcement metadata.
- [x] Optional remote predicate state in `ElasticsearchTableHandle`.
- [x] Boolean IR composition.
- [x] Table-handle serialization/round-trip coverage.
- [x] Legacy predicate state canonicalized into IR at the compatibility boundary.

Permanent invariant: all new remote predicates use the IR; no new legacy predicate maps are introduced.

## P0.2 — Native Elasticsearch `terms`

**Status:** COMPLETE — FULL TEST GATE GREEN

- [x] Single discrete value -> `Term`.
- [x] Multiple discrete values -> `Terms`.
- [x] Numeric, boolean, timestamp/date, keyword and safe `.keyword` values.
- [x] Large `IN (...)` no longer expands into bool-clause-per-value queries.
- [x] Residual retained whenever exact semantics are not guaranteed.

## P0.3 — Primitive Array Exact Pushdown

**Status:** COMPLETE — FULL TEST GATE GREEN

- [x] `contains(array, constant)` exact membership.
- [x] `arrays_overlap(array, constant_array)` exact membership.
- [x] Numeric, boolean, timestamp, IP, keyword, and safe `text.keyword` element types.
- [x] NULL/empty-array/source-NULL-element regression coverage.
- [x] Whole-array equality, positional access, sequence/cardinality operations, and analyzed-text-only membership remain residual when semantics are not provably equivalent.

## P0.4 — Dynamic Filter Planner

**Status:** COMPLETE — INTEGRATION GATE GREEN

```text
DynamicFilter
├── single exact value -> Term
├── exact set -> Terms / batched Terms
├── exact range -> Range
├── analyzed/approximate field -> no remote dynamic filter
└── excessive/unsafe -> bounded fallback
```

- [x] Configurable value, batch, and request-byte bounds.
- [x] >1,000-value coverage.
- [x] Join integration proving dynamic filtering reaches Elasticsearch and reduces source input.
- [x] Approximate analyzed-text dynamic filters are prohibited.

## P0.5 — Rule-based Predicate Migration

**Status:** COMPLETE — FULL TEST GATE GREEN

- [x] Exact discrete domain.
- [x] Range domain.
- [x] Exact LIKE/prefix.
- [x] Analyzed-text LIKE.
- [x] `starts_with`.
- [x] `substr` / `substring` prefix recognition.
- [x] `regexp_like`.
- [x] `contains`.
- [x] `arrays_overlap`.
- [x] Multiple regexp predicates can coexist in IR without map collisions.
- [x] Remote predicates survive limit/aggregation handle rewrites.

Statistics fallback for filtered handles is treated as a permanent correctness fallback: when exact filtered statistics cannot be produced, the connector may return conservative/unknown statistics. P2 extends exact statistics coverage using the same Remote Predicate IR; it does not replace this fallback policy.

## P0 final validation

Validated on PR #15 with the complete P0 test gate and GitHub CI green.

---

# P1 — Complete the Permanent Predicate and Execution Architecture

## P1.1 — `any_match` Primitive Array Pushdown

**Status:** COMPLETE — MERGED

Merged by PR #16 as commit `11829ce9e686566000d22667d8554fd3c58b8b35`.

Supported exact forms:

- [x] `any_match(a, x -> x = constant)` -> `Term`.
- [x] `any_match(a, x -> x IN (...))` -> `Term` / `Terms`.
- [x] Numeric/timestamp range predicates -> `Range`.
- [x] Exact lambda `OR` when every branch is independently exact.
- [x] Lambda `AND` only when same-element semantics can be represented safely, including fused range constraints.

Permanent invariant: same-element lambda semantics are proven inside the array predicate translator before lowering to document-level Remote Predicate IR. General P1.2 boolean composition must not bypass this boundary.

---

## P1.2 — Predicate Composition and Translation Contract

**Status:** NEXT

### Objective

Complete the permanent planner contract that composes independently translated predicates while preserving residual and enforcement semantics. This is not an `AND`-only implementation that will later be replaced by an `OR` planner; AND, OR, normalization, and residual decisions are operations of the same permanent composition layer from the beginning.

### Permanent architecture established in this phase

```text
Connector predicate
       │
       ▼
Predicate-specific translation rules
       │
       ▼
Translation result
├── Optional<RemotePredicate>
├── enforcement/exactness
├── residual requirement
└── diagnostics reason/capability
       │
       ▼
Boolean composition
├── AND
├── OR
├── normalization
└── safe NOT when proven
       │
       ▼
ElasticsearchTableHandle.remotePredicate
```

The chosen translation-result and composition APIs are permanent and will be consumed directly by P1.3 diagnostics and future predicate rules.

### P1.2a — Define translation-result invariants

- [ ] One result type represents remote predicate plus residual/enforcement information.
- [ ] EXACT means the translated subtree can be authoritative for that SQL subtree.
- [ ] PREFILTER always retains the necessary Trino residual.
- [ ] APPROXIMATE is allowed only under explicit UNSAFE semantics and remains marked in IR.
- [ ] Unsupported translation is a first-class outcome, not `null`/special-case behavior spread through `applyFilter`.
- [ ] Same-element array safety remains encapsulated in the array translator.

### P1.2b — AND composition

Examples:

```sql
name LIKE '%ngô%' AND name LIKE '%văn%'
contains(tags, 'a') AND contains(tags, 'b')
status = 'ACTIVE' AND score >= 10
```

Required behavior:

- [ ] Compose independent document-scope remote predicates under IR `And`.
- [ ] Preserve each subtree's enforcement/residual semantics.
- [ ] Flatten nested `And` nodes.
- [ ] Remove identity/single-child boolean nodes.
- [ ] Merge compatible exact range constraints only when the merge preserves semantics.
- [ ] Never reinterpret top-level multi-valued-field conjunction as same-element `any_match` semantics.

### P1.2c — OR composition

Examples:

```sql
status = 'ACTIVE' OR status = 'PENDING'
name LIKE '%foo%' OR name LIKE '%bar%'
```

Required behavior:

- [ ] Compose OR only when every branch has a remote predicate that cannot introduce false negatives for the SQL OR.
- [ ] If one OR branch is untranslatable and no safe candidate exists for it, keep the entire OR subtree residual rather than pushing only the translatable branch.
- [ ] Flatten nested `Or` nodes.
- [ ] Canonicalize same-field exact `Term`/`Terms` OR into `Terms` when semantics and request-size limits allow.
- [ ] Preserve enforcement metadata through OR composition.

Correctness invariant:

```text
A OR B
A translatable, B not translatable
=> remote A alone is NOT a valid prefilter
=> keep the OR subtree residual unless a no-false-negative candidate exists for every branch
```

### P1.2d — NOT semantics

NOT is part of the permanent composer API, but production lowering is enabled only for forms with proven SQL three-valued-logic equivalence.

- [ ] Define NULL/missing semantics for scalar and multi-valued fields.
- [ ] Prove whether forms require `Exists(field) AND NOT(predicate)`.
- [ ] Add exact NOT forms only after proof and tests.
- [ ] Unsupported NOT remains a normal residual outcome of the same composer; no temporary alternative NOT implementation is introduced.

### P1.2e — Planner cleanup

- [ ] `ElasticsearchMetadata.applyFilter` orchestrates translation/composition but does not contain predicate-specific boolean special cases.
- [ ] No new synthetic `TupleDomain` bridge for new predicates.
- [ ] No new parallel regex/prefix/full-text maps.
- [ ] Existing compatibility state is canonicalized at the boundary and not used as a foundation for P1.2 functionality.

### Required tests

Unit:

- [ ] Translation-result invariants.
- [ ] EXACT/PREFILTER/APPROXIMATE composition matrix.
- [ ] AND/OR normalization.
- [ ] Partial-OR rejection.
- [ ] Same-field range merge safety.
- [ ] Document-scope vs same-element array regression.
- [ ] NULL/missing behavior for any enabled NOT form.

Planner:

- [ ] Correct residual retained/removed.
- [ ] Table handle contains the expected composed IR.
- [ ] Repeated `applyFilter` calls compose with existing handle predicates rather than replacing them.

DSL:

- [ ] IR renders to one deterministic Elasticsearch bool tree.
- [ ] Large `Terms` normalization respects request limits.

ES7/ES8 integration:

- [ ] Same-field full-text AND.
- [ ] Same-field exact AND.
- [ ] Exact OR.
- [ ] Mixed translatable/untranslatable OR remains correct.
- [ ] Array membership conjunction.
- [ ] `any_match` same-element regression.
- [ ] NULL/missing documents.
- [ ] Result comparison against reference Trino execution.

### P1.2 completion gate

P1.2 is complete only when the composition API is suitable unchanged for P1.3 diagnostics and future predicate translators. If P1.3 would require replacing the result/composer model, P1.2 is not complete.

---

## P1.3 — Permanent Pushdown Diagnostics and Observability

**Status:** NOT STARTED

### Objective

Create one durable diagnostics/event model emitted from the permanent translation, composition, rendering, and execution contracts. Logs and metrics are consumers of this model; they are not separate instrumentation architectures.

### Permanent diagnostic model

```text
Pushdown diagnostics
├── input predicate category
├── translation outcome
├── enforcement: EXACT / PREFILTER / APPROXIMATE
├── residual retained/removed
├── normalization decisions
├── remote predicate node counts
├── terms value/batch counts
├── dynamic-filter counts
├── generated request/query characteristics
└── execution counters
```

### Scope

- [ ] Translation/composition diagnostic events use stable structured reason codes, not ad-hoc log strings.
- [ ] Remote Predicate IR -> Elasticsearch DSL diagnostics use the same model.
- [ ] EXACT / PREFILTER / APPROXIMATE / residual counts.
- [ ] `terms` values, batches, and request-byte estimates.
- [ ] Array-membership and `any_match` pushdown counts.
- [ ] Dynamic-filter values received/pushed/compacted/batched/rejected.
- [ ] Remote requests, rows, bytes, pages, retries, cancellations, and failures where available.
- [ ] Debug logging can render diagnostics without changing planner behavior.
- [ ] Metrics/JMX consumers read the same counters/events.

### Architectural continuity

P1.4 search execution strategies and P2 aggregation/statistics paths must emit into this same diagnostic/accounting contract. They may add fields/counters but must not create replacement metrics pipelines.

---

## P1.4 — Search Execution Framework and Large-scan Optimization

**Status:** NOT STARTED

### Objective

Establish one permanent search-execution lifecycle and then optimize large scans by adding strategies and decoders behind it. Scroll is not a temporary phase implementation and PIT is not a replacement architecture.

### P1.4a — Stable search execution lifecycle

Define the long-lived contract used by every scan strategy:

```text
SearchExecutionStrategy
├── capability check
├── open query context
├── fetch next page
├── expose page/request accounting
├── cancellation
├── retry/failure classification
└── close query context
```

- [ ] Adapt current Scroll execution to this contract without semantic changes.
- [ ] Centralize clear-scroll/close behavior.
- [ ] Centralize cancellation and failure cleanup.
- [ ] Emit P1.3 diagnostics/accounting from the common lifecycle.
- [ ] Preserve ES-version capability handling explicitly.

### P1.4b — Stable hit-decoding contract

Define one hit/page decoding interface consumed by all search strategies.

- [ ] Existing JsonNode/materialized decoding works through the interface first.
- [ ] Add Jackson streaming decoding as another implementation when benchmark/tests prove benefit.
- [ ] Decoder selection must not change planner or search-strategy contracts.
- [ ] Validate nested objects, arrays, NULL/missing, timestamps, binary, and raw JSON.

### P1.4c — PIT + `search_after` as an additional strategy

- [ ] Benchmark Scroll through the common execution contract.
- [ ] Implement PIT + `search_after` through the same contract where supported.
- [ ] Benchmark throughput, heap, GC, request count, failure recovery, cancellation, and long-query stability.
- [ ] Add capability/configuration-based selection policy.
- [ ] Keep Scroll as a valid fallback strategy unless a future compatibility decision explicitly removes support; removing an implementation must not alter the execution contract.

### Acceptance criteria

- Planner and page-source code depend on the strategy/lifecycle interface, not a specific pagination mechanism.
- Changing strategy does not change SQL semantics or predicate planning.
- Cancellation closes all remote contexts.
- Accounting is comparable across Scroll and PIT.
- No optimization requires a later phase to replace the P1.4 execution abstraction.

---

# P2 — Query-shape, Aggregation, and Statistics Hardening

**Status:** NOT STARTED

P2 extends the permanent P0/P1 planner and execution contracts. It does not introduce a second predicate model, diagnostics path, or scan lifecycle.

## P2.1 — LIMIT and TopN execution hardening

- [ ] Review shard-aware LIMIT/TopN over-fetch correctness.
- [ ] Express early termination through the P1.4 execution lifecycle.
- [ ] Ensure remote predicate, sort, and limit state are preserved consistently in table handles.
- [ ] Add cancellation as soon as the exact required row set is known.
- [ ] Measure request/page/byte reduction through P1.3 accounting.
- [ ] Keep fallback behavior exact when a remote TopN cannot be represented safely.

## P2.2 — Aggregation execution hardening

- [ ] Use the same Remote Predicate IR renderer for aggregation query filters.
- [ ] Add common request/byte/page/resource accounting through P1.3 diagnostics.
- [ ] Make composite aggregation page size configurable only if benchmarks justify it.
- [ ] Centralize aggregation pagination/resource cleanup behind a stable aggregation execution contract if more than one execution mode is required; do not add an abstraction speculatively.
- [ ] Preserve correctness restrictions for filtered, ordered, and exact DISTINCT aggregates.

## P2.3 — Statistics with Remote Predicate IR

Permanent policy:

```text
Can compute exact/useful statistics for current handle
    -> return them
Cannot prove statistics semantics or cost is unsafe
    -> return conservative unknown/empty statistics
```

The conservative fallback remains valid permanently. This phase extends the set of handles for which useful statistics can be produced.

- [ ] Render supported exact Remote Predicate IR into statistics/count requests.
- [ ] Never ignore a table handle's remote predicate.
- [ ] Keep PREFILTER/APPROXIMATE statistics conservative unless semantics are explicitly defined.
- [ ] Add request/document-count bounds.
- [ ] Measure planning overhead before adding caches.
- [ ] Add caching/selectivity improvements only through the same statistics contract.

## P2.4 — Cross-feature invariants

- [ ] Predicate + LIMIT.
- [ ] Predicate + TopN.
- [ ] Predicate + aggregation.
- [ ] Predicate + statistics.
- [ ] Dynamic filter + LIMIT/TopN where planner order permits.
- [ ] Projection/handle rewrites preserve all remote state.
- [ ] ES7/ES8 result equivalence and resource cleanup.

---

# P3 — Optional SPI Extensions

**Status:** NOT STARTED

## `applySample`

Research only after P0/P1/P2. Elasticsearch sampling semantics must be proven against Trino TABLESAMPLE semantics before any production pushdown is added. If implemented, it must extend the existing table-handle/query-shape and execution contracts rather than creating a side execution path.

## `applyJoin`

Do not prioritize generic join pushdown. Preferred architecture remains:

```text
Trino join
  -> build-side dynamic filter
  -> DynamicFilterPlanner
  -> exact Term/Terms/Range Remote Predicate IR
  -> SearchExecutionStrategy
  -> Elasticsearch
```

A future join-pushdown proposal must first demonstrate that it can reuse the permanent predicate, diagnostics, and execution contracts without replacing them.

---

# Current execution order

```text
P0 COMPLETE / GREEN
  │
  ▼
P1.1 any_match COMPLETE / MERGED
  │
  ▼
P1.2 Predicate Composition + Translation Contract   <- NEXT
  │
  ▼
P1.3 Permanent Diagnostics / Observability
  │
  ▼
P1.4 Stable Search Execution Framework
  │       ├── Scroll strategy
  │       ├── stable decoder contract
  │       └── PIT + search_after strategy after benchmarks
  ▼
P2 Query-shape / Aggregation / Statistics Hardening
  │
  ▼
P3 Optional SPI Extensions
```

## Phase-start checklist

Before coding any future phase, record answers to all of the following in the branch/PR description or roadmap update:

- [ ] Which permanent abstractions does this phase use?
- [ ] Does it introduce a new permanent abstraction? Why is the current architecture insufficient?
- [ ] How do all currently known later phases extend this abstraction without replacing it?
- [ ] Is any code being introduced with a known removal/replacement phase? If yes, redesign before implementation.
- [ ] Are compatibility bridges isolated from new functionality?
- [ ] Are exactness, residual, NULL/missing, multi-valued-field, cancellation, and resource-lifecycle semantics defined?
- [ ] What focused, connector, ES7/ES8, and CI gates prove completion?

Update this file after every completed stage, including the exact tests/CI used to validate completion.
