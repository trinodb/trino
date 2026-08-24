# Elasticsearch Pushdown Architecture Regression Gate

This document defines the mandatory test protocol for any Elasticsearch connector change that alters planner, predicate IR, table-handle state, execution lifecycle, or another architectural boundary.

A green test suite is not sufficient when the architecture changed. Existing tests must be re-audited to ensure they still validate production semantics rather than an obsolete implementation.

## Mandatory rule

When a change modifies an architectural contract, the phase owner must perform all of the following before the phase can be marked complete:

1. Inventory the entire `plugin/trino-elasticsearch` test suite, not only tests changed by the PR.
2. Identify every test that touches the changed architectural boundary directly or indirectly.
3. Classify each affected test as one of:
   - **semantic contract** — validates SQL/Elasticsearch correctness and remains valid;
   - **current architecture** — validates a permanent production abstraction and remains valid;
   - **compatibility boundary** — validates a production compatibility path that still executes;
   - **obsolete implementation** — validates a retired representation/path and must be removed or rewritten.
4. Never update an expected value merely to make an obsolete test pass. First decide whether the behavior represented by the test is still a production contract.
5. Remove tests for dead compatibility helpers and retired intermediate representations. Replace them with tests at the permanent abstraction that owns the behavior now.
6. Re-run the complete connector test suite after the audit, including both Elasticsearch 7 and Elasticsearch 8 integration suites.
7. Review test coverage after execution. A passing suite that no longer exercises a migrated behavior is a regression in test quality.

## Pushdown correctness invariants

These invariants apply to every architecture phase and are stronger than a simple “keep a residual” rule.

### Remote candidate must be lossless

A Trino residual can eliminate false positives returned by Elasticsearch. It cannot recover a row that an Elasticsearch pre-filter already removed.

Therefore:

```text
PREFILTER
  => remote predicate may be a superset of SQL matches
  => remote predicate MUST NOT introduce false negatives
  => Trino residual remains authoritative

APPROXIMATE
  => false positives and/or false negatives may exist
  => allowed only under explicit UNSAFE semantics
```

The presence of a residual never makes a lossy remote predicate safe. Analyzer-dependent text transformations, stop-word removal, stemming, case folding, ASCII folding, tokenization, or other index-time rewriting require an explicit no-false-negative proof before they can be used as PREFILTER candidates.

### Planner ownership cannot be bypassed by compatibility code

`remaining` and `residual` are different architectural outcomes:

```text
remaining
  => planner does not own the subtree
  => compatibility boundary may inspect/translate it

residual
  => planner owns the subtree and deliberately keeps Trino authoritative
  => compatibility boundary MUST NOT retry or reinterpret it
```

Partial OR, unproven NOT, resource-budget rejection, and recognized full-text forms deliberately rejected by SAFE are planner-owned residuals. A legacy path must never get a second chance to push a predicate that the permanent planner rejected for correctness.

### Document scope must not be collapsed into same-value scope

Elasticsearch multi-valued fields do not preserve the distinction between a Trino scalar column and a remotely multi-valued field strongly enough for a global IR normalizer to assume one-value semantics.

For example, a document with values `[5, 25]` satisfies these independent document-scope predicates:

```text
Range(field > 10)
AND
Range(field < 20)
```

because different values may satisfy each clause. Rewriting them globally to `Range(10 < field < 20)` would create a false negative.

Therefore the global Remote Predicate IR normalizer may flatten and deduplicate boolean structure, but it must not fuse independent ranges merely because they use the same field name. Range fusion is allowed only at an abstraction that has explicit same-value proof, such as the `any_match` lambda translator after it proves both bounds apply to the same array element.

## Required coverage layers

Architecture-changing PRs must cover all applicable layers:

### 1. Translation/IR unit tests

Validate:

- translation outcome and reason;
- EXACT/PREFILTER/APPROXIMATE enforcement;
- `remaining` versus planner-owned `residual`;
- IR normalization without semantic changes;
- serialization/round-trip where table-handle state is involved.

### 2. Planner/composer tests

Validate:

- complete and partial AND;
- complete and partial OR;
- EXACT/PREFILTER/APPROXIMATE composition matrix;
- residual retention/removal;
- repeated `applyFilter` fixed points;
- existing remote predicate composition;
- document-scope versus same-element array semantics;
- independent same-field ranges are not fused without same-value proof;
- resource-budget fallback;
- unsupported NOT behavior until NULL/missing equivalence is proven;
- rejected planner-owned predicates cannot fall back through legacy metadata.

### 3. Metadata boundary tests

`TestRuleBasedElasticsearchMetadata` tests only runtime orchestration and compatibility boundaries that are still reachable in production.

Predicate recognition or retired synthetic-domain lowering must not be tested here merely because this class historically contained that implementation.

### 4. DSL rendering tests

Validate the generated Elasticsearch query structure for composed IR, including deterministic bool trees, `terms` batching, ranges, full-text predicates, and enforcement wrappers.

### 5. Elasticsearch 7 and 8 acceptance tests

Every architecture migration affecting remote predicates must execute against both supported Elasticsearch generations and compare SQL results with Trino semantics.

Required regression families include:

- scalar equality/range/LIKE/regexp;
- keyword and analyzed-text behavior;
- analyzers that rewrite source text, specifically to detect false-negative PREFILTER behavior;
- same-field and cross-field conjunctions;
- exact and proven-prefilter OR;
- mixed translatable/untranslatable OR;
- primitive-array membership;
- `any_match` same-element constraints;
- NULL, missing field, empty array, and source-array NULL elements;
- case-preserving remote field names;
- large `IN`/`Terms` and request-budget fallbacks;
- dynamic filters;
- limit/aggregation handle preservation where remote predicate state is present.

## P1.2 audit decisions

P1.2 changes the predicate translation/composition architecture. The following audit decisions therefore apply:

- `ElasticsearchPredicateTranslation` and `ElasticsearchPredicateComposer` are the primary semantic test targets.
- Partial OR, unproven NOT, resource rejection, and recognized but unproven SAFE full-text translations are planner-owned residuals. Tests must not expect them to return through the legacy compatibility boundary.
- SAFE analyzed-text equality/LIKE is not remotely prefiltered without a no-false-negative proof. UNSAFE remains the explicit approximation boundary.
- `ElasticsearchRemotePredicateNormalizer` performs deterministic semantic normalization only: flattening, deduplication, and recursive normalization. It does not fuse independent same-field ranges because document-scope predicates may be satisfied by different remote values.
- Same-value range fusion remains encapsulated in translators that can prove that semantic scope, including the existing `any_match` translator.
- Contradictory independent ranges are not encoded using a temporary fake match-none expression. They remain valid independent clauses until a permanent IR representation and same-value proof justify a stronger rewrite.
- `TestRuleBasedElasticsearchMetadata` is restricted to facade/runtime fixed-point, residual orchestration, and compatibility-bypass regression tests.
- Historical tests for synthetic `TupleDomain` full-text lowering are obsolete once runtime lowering targets Remote Predicate IR directly; those tests and their test-only production helper must not be preserved as architectural requirements.
- P0 and P1.1 acceptance tests remain mandatory regression inputs and are inherited by the P1.2 Elasticsearch 7/8 suites.

## Completion commands

Focused tests may be used during implementation, but the final architecture gate always includes the full module:

```bash
./mvnw -pl :trino-elasticsearch -Dtest=<affected-test-class> test
./mvnw -pl :trino-elasticsearch airstyle:check
./mvnw -pl :trino-elasticsearch test
```

The GitHub CI matrix must then complete successfully. Connector test failures, compile failures, AirStyle failures, Error Prone failures, Elasticsearch 7 failures, and Elasticsearch 8 failures are analyzed independently by root cause.

## Phase completion evidence

An architecture-changing phase is complete only when the PR records:

- which permanent contracts changed;
- which existing test classes were audited;
- which obsolete tests were removed or rewritten and why;
- which new tests replaced their semantic coverage;
- full connector test result;
- Elasticsearch 7 result;
- Elasticsearch 8 result;
- confirmation that the next roadmap phase can consume the architecture without replacing it.
