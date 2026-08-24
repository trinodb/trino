# P1.2 Full Test Architecture Audit

This audit is required because P1.2 changes the Elasticsearch predicate translation and boolean-composition architecture. The goal is not merely to keep tests green. Each existing test source is checked against the production architecture so obsolete implementation tests do not masquerade as regression coverage.

## Acceptance inheritance

The Elasticsearch 7 and 8 suites use the same cumulative acceptance hierarchy:

```text
TestElasticsearch7ConnectorTest / TestElasticsearch8ConnectorTest
        ↓
BaseElasticsearchPredicateCompositionTest     (P1.2)
        ↓
BaseElasticsearchAnyMatchPushdownTest         (P1.1)
        ↓
BaseElasticsearchP0PredicatePushdownTest      (P0)
        ↓
BaseElasticsearchFullTextPushdownTest
        ↓
BaseElasticsearchConnectorTest
        ↓
Trino BaseConnectorTest
```

Therefore adding P1.2 does not replace P0/P1.1 acceptance coverage. Both Elasticsearch generations rerun the complete inherited behavior contract.

## Classification vocabulary

- **CURRENT-SEMANTIC** — validates SQL/Elasticsearch behavior independent of an implementation detail.
- **CURRENT-ARCH** — validates a permanent production abstraction introduced or retained by the current architecture.
- **COMPATIBILITY** — validates a compatibility boundary that is still reachable in production and therefore must remain tested.
- **SUPERSEDED** — test definition remains in an ancestor but current suites override it with the newer contract; it is not counted as current coverage.
- **INFRASTRUCTURE** — test fixture/support code, compiled and exercised by integration tests but not itself a behavior contract.
- **INDEPENDENT** — unrelated to predicate-composition architecture; unchanged but mandatory in the full module regression run.
- **REWRITTEN** — old implementation-based assertion was migrated to the new permanent architecture.
- **REMOVED-OBSOLETE** — old implementation path no longer executes in production; test was removed rather than changing expected output.
- **ADDED-GAP** — architecture audit found a permanent contract without direct test coverage, so a new test was added.

## Complete inventory

| # | Test source | Classification | P1.2 audit decision |
|---:|---|---|---|
| 1 | `BaseElasticsearchAnyMatchPushdownTest.java` | CURRENT-SEMANTIC / CURRENT-ARCH | Keep. P1.1 same-element semantics are mandatory regression coverage under P1.2. |
| 2 | `BaseElasticsearchConnectorTest.java` | CURRENT-SEMANTIC + SUPERSEDED definitions | Keep as inherited baseline. Historical predicate methods overridden by FullText/P0 are not counted as current predicate coverage. |
| 3 | `BaseElasticsearchFullTextPushdownTest.java` | CURRENT-SEMANTIC | Keep and re-evaluate SAFE expectations against the lossless-prefilter invariant. UNSAFE remains the analyzer-semantic opt-in. |
| 4 | `BaseElasticsearchP0PredicatePushdownTest.java` | CURRENT-SEMANTIC / CURRENT-ARCH | Keep. Native Terms, dynamic filtering, array membership, NULL edges and same-field regexp behavior must survive P1.2. |
| 5 | `BaseElasticsearchPredicateCompositionTest.java` | CURRENT-SEMANTIC / CURRENT-ARCH / REWRITTEN | P1.2 ES7/ES8 contract. Expanded with exact same-field scalar composition, UNSAFE same-field full-text AND and a custom-analyzer regression proving SAFE cannot use a lossy candidate. |
| 6 | `ElasticsearchLoader.java` | INFRASTRUCTURE | No architecture assertion to rewrite. Still compiled/used by connector tests. |
| 7 | `ElasticsearchQueryRunner.java` | INFRASTRUCTURE | No architecture assertion to rewrite. Required by integration suite. |
| 8 | `ElasticsearchServer.java` | INFRASTRUCTURE | No architecture assertion to rewrite. Required by ES7/ES8 integration suite. |
| 9 | `TestAggregationQueryPageSource.java` | INDEPENDENT | Aggregation page decoding is not changed by P1.2; still mandatory in full module run. |
| 10 | `TestAwsSecurityConfig.java` | INDEPENDENT | Security configuration unaffected; keep and rerun. |
| 11 | `TestBuildSort.java` | INDEPENDENT | Sort construction unaffected; keep and rerun. |
| 12 | `TestElasticsearch7ConnectorTest.java` | CURRENT-ARCH | Entry point for cumulative P1.2→P1.1→P0→FullText→Base acceptance on ES7. |
| 13 | `TestElasticsearch8ConnectorTest.java` | CURRENT-ARCH | Entry point for cumulative P1.2→P1.1→P0→FullText→Base acceptance on ES8. |
| 14 | `TestElasticsearchArrayPredicateTranslator.java` | CURRENT-ARCH | Owns same-element proof boundary for `any_match`; document-level composer must not reinterpret these tests. |
| 15 | `TestElasticsearchComplexTypePredicatePushDown.java` | CURRENT-SEMANTIC | Keep. Nested primitive/ROW/ARRAY predicate and no-data-read behavior remains broad regression coverage. |
| 16 | `TestElasticsearchConfig.java` | INDEPENDENT / CONFIG | Keep. Existing dynamic-filter/resource configuration remains mandatory. |
| 17 | `TestElasticsearchDynamicFilterPlanner.java` | CURRENT-ARCH | Keep. Dynamic filters remain exact-only; P1.2 normalizer must not turn batching into approximate behavior. |
| 18 | `TestElasticsearchMetadata.java` | CURRENT-SEMANTIC | Keep. `LIKE`→regexp helper semantics remain used by current exact/UNSAFE translation paths. |
| 19 | `TestElasticsearchPredicateComposer.java` | CURRENT-ARCH / REWRITTEN | Expanded to lock EXACT/PREFILTER/APPROXIMATE algebra, partial OR ownership, whole-OR residual behavior and exact Terms compaction. |
| 20 | `TestElasticsearchPredicateCompositionPlanner.java` | CURRENT-ARCH / REWRITTEN | Locks document-scope composition, owned residuals, lossless SAFE analyzed-text rejection and mixed EXACT/proven-PREFILTER OR. |
| 21 | `TestElasticsearchPredicateCompositionPolicy.java` | CURRENT-ARCH / REWRITTEN | Explicitly tests all resource limits and planner-owned fallback instead of relying on hidden defaults. |
| 22 | `TestElasticsearchPredicateCompositionRequestBudget.java` | CURRENT-ARCH / REWRITTEN | Oversized composed predicates become owned residuals rather than compatibility state or oversized remote requests. |
| 23 | `TestElasticsearchPredicatePushdownPlanner.java` | CURRENT-ARCH / REWRITTEN | Direct planner→IR contract. SAFE analyzed Domain/LIKE now remain owned residuals; proven keyword regexp candidate remains PREFILTER. |
| 24 | `TestElasticsearchPredicateTranslation.java` | CURRENT-ARCH / ADDED-GAP | Added by audit to validate result invariants and the semantic difference between `remaining` and planner-owned `residual`. |
| 25 | `TestElasticsearchProjectionPushdownPlans.java` | CURRENT-ARCH | Remote predicate state must survive projection/dereference and join planning. |
| 26 | `TestElasticsearchQueryBuilder.java` | COMPATIBILITY + REMOVED-OBSOLETE | Generic TupleDomain compatibility rendering remains; obsolete analyzed-text synthetic-domain renderer tests were removed. |
| 27 | `TestElasticsearchRemoteColumnCase.java` | CURRENT-ARCH / REWRITTEN | Migrated analyzed-text casing coverage from legacy TupleDomain query building to planner→Remote Predicate IR. |
| 28 | `TestElasticsearchRemotePredicateNormalizer.java` | CURRENT-ARCH / ADDED-GAP / REWRITTEN | Added by audit. Locks flatten/dedupe and explicitly proves independent same-field ranges are preserved at document scope; range fusion is not a global IR rewrite. |
| 29 | `TestElasticsearchRemotePredicateQueryBuilder.java` | CURRENT-ARCH | Canonical DSL renderer for Term/Terms/Range/Prefix/Regexp/MatchPhrase/MatchPhrasePrefix/Exists/And/Or/Not/Enforced. |
| 30 | `TestElasticsearchRemotePredicateTranslator.java` | CURRENT-ARCH + COMPATIBILITY | Current Domain translation plus legitimate legacy-state canonicalization into IR. |
| 31 | `TestElasticsearchTableHandle.java` | CURRENT-ARCH + COMPATIBILITY | IR serialization, connector-handle round trip and copy preservation; legacy construction only where still supported. |
| 32 | `TestLikePrefix.java` | CURRENT-SEMANTIC | Pure LIKE-prefix recognition helper used by current planner. |
| 33 | `TestPasswordConfig.java` | INDEPENDENT | Authentication configuration unaffected; keep and rerun. |
| 34 | `TestRegexpPushdownTranslator.java` | CURRENT-SEMANTIC | Exact/approximate/unsupported regexp classification remains current production behavior. |
| 35 | `TestRuleBasedElasticsearchMetadata.java` | CURRENT-ARCH / REWRITTEN / REMOVED-OBSOLETE | Restricted to facade fixed-point/orchestration; proves SAFE planner rejection cannot be bypassed and repeated `applyFilter` preserves independent document-scope ranges. |
| 36 | `client/TestExtractAddress.java` | INDEPENDENT | Client address parsing unaffected; keep and rerun. |
| 37 | `client/TestKeywordSubfield.java` | CURRENT-SEMANTIC | Exact-predicate safety of keyword sub-fields directly affects planner field selection. |

## Architectural findings from the audit

### 1. Retired synthetic full-text lowering bridge removed

Old tests in `TestRuleBasedElasticsearchMetadata` directly exercised `rewriteUnsafeFullTextConstraint()` and asserted temporary synthetic Domain transport. Runtime planning already targets `ElasticsearchRemotePredicate` directly, so keeping the helper solely for tests would preserve dead architecture.

The tests were removed/migrated and the production helper was deleted. `createLikePrefixDomain()` remains in the predicate planner because it identifies a prefix range synthesized by Trino `DomainTranslator` on the UNSAFE analyzed-prefix path; the metadata facade contains no predicate-specific normalization logic.

Replacement coverage lives at the owning abstractions:

- `TestElasticsearchPredicateTranslation`
- `TestElasticsearchPredicatePushdownPlanner`
- `TestElasticsearchPredicateComposer`
- `TestElasticsearchPredicateCompositionPlanner`
- `TestElasticsearchRemotePredicateQueryBuilder`
- cumulative ES7/ES8 acceptance suites

### 2. SAFE residual did not guarantee correctness

The audit found a semantic contradiction in the previous SAFE full-text contract. A Trino residual can remove remote false positives, but cannot recover a SQL match already filtered out by Elasticsearch.

Concrete regression case:

```text
source value: "ngô văn"
analyzer: standard + lowercase + asciifolding
indexed terms: "ngo", "van"
SQL: name LIKE '%ngô%'
```

A remote regexp containing `ngô` can miss the indexed `ngo` term. Retaining the SQL LIKE as a residual cannot restore the lost row.

P1.2 therefore makes SAFE genuinely lossless:

```text
SAFE analyzed-text Domain/LIKE without proof
  -> no remote predicate
  -> planner-owned residual
  -> legacy compatibility must not retry it

SAFE proven keyword regexp candidate
  -> remote PREFILTER
  -> exact SQL residual retained

UNSAFE analyzed-text translation
  -> APPROXIMATE remote predicate
  -> analyzer semantics explicitly accepted
```

`BaseElasticsearchPredicateCompositionTest` now includes a custom analyzer acceptance case on both ES7 and ES8 that would fail under the old lossy SAFE implementation.

### 3. Planner rejection cannot fall through to legacy metadata

Returning a recognized-but-rejected SAFE predicate as `remaining` was insufficient because legacy `ElasticsearchMetadata.applyFilter()` can also translate analyzed-text predicates. That allowed the compatibility layer to bypass the permanent planner's correctness decision.

The final ownership contract is:

```text
remaining
  -> planner does not own predicate
  -> compatibility may inspect it

residual
  -> planner owns predicate
  -> Trino is authoritative
  -> compatibility may not retry it
```

This rule now covers partial OR, unproven NOT, resource-budget fallback and recognized SAFE full-text forms without lossless proof.

### 4. Enforcement algebra is explicit regression coverage

Composer tests now lock the effective subtree semantics:

```text
EXACT + EXACT       -> EXACT
EXACT + PREFILTER   -> PREFILTER + required residual
EXACT + APPROXIMATE -> APPROXIMATE

OR with PREFILTER   -> whole SQL OR retained as residual
OR with APPROXIMATE -> APPROXIMATE dominates
partial OR          -> no remote OR; whole subtree is owned residual
```

An APPROXIMATE branch is allowed only under explicit UNSAFE policy. A residual does not convert an approximation into a safe candidate.

### 5. Document-scope ranges are deliberately not fused globally

The audit briefly introduced global same-field numeric range intersection, then rejected it after checking Elasticsearch multi-value semantics.

For a document containing remote values `[5, 25]`, these independent clauses both match:

```text
field > 10
field < 20
```

because `25` satisfies the first and `5` satisfies the second. Globally rewriting them to a single `10 < field < 20` range would eliminate that document and introduce a false negative.

The permanent rule is therefore:

```text
Remote Predicate IR normalizer
  -> may flatten/deduplicate document-scope AND/OR
  -> MUST preserve independent same-field Range clauses

same-value semantic scope
  -> may fuse ranges only after explicit proof
  -> existing example: any_match lambda translator
```

`TestElasticsearchRemotePredicateNormalizer` and `TestRuleBasedElasticsearchMetadata` now lock this behavior, including repeated `applyFilter` calls over an existing remote predicate.

This decision intentionally avoids a temporary `MatchNone` workaround for contradictory independent ranges. A permanent `MatchNone` node can be added later only if a planner scope has enough proof to use it correctly; it is not needed to complete P1.2.

### 6. Legacy QueryBuilder full-text transport removed from current coverage

Old `TestElasticsearchQueryBuilder` cases manufactured analyzed-text TupleDomain/legacy prefix-map state solely to render `match_phrase` or `match_phrase_prefix`. These are no longer the production transport for new predicate functionality and were removed.

Canonical full-text DSL rendering is tested directly by `TestElasticsearchRemotePredicateQueryBuilder` and end-to-end by the inherited acceptance suites.

### 7. Remote field casing migrated to current architecture

The analyzed-text case-preservation test now protects:

```text
Constraint
  -> ElasticsearchPredicatePushdownPlanner
  -> Remote Predicate IR using original remote field case
```

rather than manufacturing a legacy analyzed-text Domain solely for the old renderer.

## Compatibility paths intentionally retained

P1.2 does not delete all legacy state atomically. The following compatibility coverage remains intentional:

- legacy predicate state canonicalized into `ElasticsearchRemotePredicate` at the `RuleBasedElasticsearchMetadata` boundary;
- generic TupleDomain state rendered together with an already planned remote predicate;
- legacy table-handle constructor/serialization behavior where still supported.

No new P1.2 feature is implemented on top of these compatibility paths.

## Architecture-sensitive test groups to pass before completion

The final P1.2 head must pass:

1. Translation and IR
   - `TestElasticsearchPredicateTranslation`
   - `TestElasticsearchPredicatePushdownPlanner`
   - `TestElasticsearchArrayPredicateTranslator`
   - `TestElasticsearchRemotePredicateTranslator`
   - `TestElasticsearchRemotePredicateNormalizer`
   - `TestElasticsearchRemotePredicateQueryBuilder`
   - `TestElasticsearchTableHandle`

2. Composition
   - `TestElasticsearchPredicateComposer`
   - `TestElasticsearchPredicateCompositionPlanner`
   - `TestElasticsearchPredicateCompositionPolicy`
   - `TestElasticsearchPredicateCompositionRequestBudget`

3. Metadata/planner boundaries
   - `TestRuleBasedElasticsearchMetadata`
   - `TestElasticsearchProjectionPushdownPlans`
   - `TestElasticsearchRemoteColumnCase`

4. Dynamic filtering/resource safety
   - `TestElasticsearchDynamicFilterPlanner`

5. Full Elasticsearch acceptance
   - `TestElasticsearch7ConnectorTest`
   - `TestElasticsearch8ConnectorTest`
   - inherited FullText/P0/P1.1/P1.2 behavior
   - custom analyzer SAFE false-negative regression

6. Entire module
   - every architecture-independent test in the complete inventory above

## Final completion gate

P1.2 must not be marked complete until all of these are true:

- all 37 test/support sources above have been reviewed against the final production architecture;
- obsolete implementation tests and test-only production helpers have been removed or migrated;
- no current behavior is covered only by a superseded ancestor test;
- focused architecture-sensitive tests pass;
- AirStyle passes;
- the complete `:trino-elasticsearch` module test suite passes;
- Elasticsearch 7 cumulative acceptance passes;
- Elasticsearch 8 cumulative acceptance passes;
- Error Prone/compile checks pass;
- final CI runs on a stable head that contains the final production semantics and regression suite;
- P1.3 can consume `ElasticsearchPredicateTranslation`, composer, normalized IR, enforcement and reason metadata without replacing the P1.2 model.
