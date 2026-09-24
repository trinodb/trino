# Relational predicate pushdown

Relational predicate pushdown moves filters towards their inputs and infers
constraints from joins. It runs before connector predicate pushdown, which
translates a filter immediately above a table scan into a connector constraint.
The two operations have separate responsibilities and can run at several points
in the optimizer pipeline.

`PlanOptimizers` constructs the relational pushdown rule collections and reuses
them in the corresponding optimizer phases. Each rule is a separate class in
the iterative-rule package.
Each filter rule matches a filter over one specific source-node type, captures
that source through the memo-aware pattern matcher, and performs one local
rewrite. Each rule owns its transformation; shared helpers perform predicate
analysis and check whether the rewrite made progress.
Separate join and spatial-join rules propagate effective input predicates
even when the join has no parent filter. The iterative optimizer visits the
children and revisits their parents after changes.

The iterative implementation is enabled by default. During the transition,
`optimizer.iterative-predicate-pushdown.enabled=false` selects the legacy
`PredicatePushDown` visitor. The `iterative_predicate_pushdown_enabled` session
property overrides this configuration default. Each of the seven relational
pushdown phases selects exactly one implementation using the iterative
optimizer's legacy fallback mechanism, with the same phase-specific settings.
Six phases share a memo and fixed-point iteration with the following cleanup or
transformation stage. The pre-join-reordering phase remains separate from column
pruning. The fallback runs the visitor and then each following stage as separate
optimizers, preserving their original order.
Connector pushdown and dynamic-filter enablement remain independently controlled.

The rules preserve the existing restrictions on predicate movement. Examples
include partition keys for windows, common grouping columns for grouping sets,
replicate symbols for unnest, and null-rejection analysis for outer joins.
Global aggregations and unsupported operators remain barriers. Projection
substitution preserves the restrictions on nondeterministic assignments and
duplicated expressions. Join inference preserves the `allow_unsafe_pushdown`
setting and the handling of expressions that can fail.

## Effective predicates and convergence

An effective predicate describes facts guaranteed by an operator's output.
`EffectivePredicateExtractor` resolves memo group references while traversing
the plan. It reads the current group contents on each extraction; results are
not cached across group replacements. Runtime dynamic-filter expressions are
excluded from this static analysis.
For table scans, connector table predicates are intersected with the scan's
enforced constraint. Connector properties can conservatively report no predicate
even after accepting a pushed filter. Convergence checks also consult the exact
enforced constraint because effective-predicate extraction can simplify large
discrete domains to ranges.

A successful iterative rewrite must make progress. Inferred predicates already
present at an input are not inserted again. Effective-predicate extraction alone
is insufficient for this check: a filter pushed through a projection can change
form during cast unwrapping and become unavailable to upward inference. The
rules also recognize existing filters through the supported input mappings.
These filters use the same normalization and implication checks as inferred
predicates, so stronger or differently expressed constraints prevent reinsertion.
For nested joins, including spatial joins, the check follows either input of an
inner join and only the preserved input of an outer join. It does not use input
predicates for null-extended columns or propagate them through a full join.
An inner join's own filter, including a spatial join condition, also guarantees
predicates on its output, including predicates involving both inputs. Outer-join
filters provide no such guarantee for unmatched rows.
Predicates are simplified after projection substitution so a predicate that
becomes `TRUE`, such as a comparison satisfied by every `CASE` branch, is not
reinserted after its filter disappears.
For Values inputs, the check evaluates the predicate against each deterministic,
uncorrelated row, within the `push_filter_into_values_max_row_count` limit. This
recognizes predicates already consumed by `PushFilterIntoValues`, including
expressions that domain inference cannot prove. Every row must satisfy the
predicate before an inferred filter can be omitted.
`PredicateEnforcement` performs this read-only implication analysis with a plan
visitor. It examines current alternatives with `Lookup.resolveGroup` because a
proof can follow an arbitrary number of input mappings. It retains no proof
across group replacements and never constructs or replaces plan nodes.

Structural preconditions for rewriting belong to the individual rules. The
existing `MergeFilters` rule matches a filter directly above another filter and
captures its child, preserving inner-to-outer conjunct order. The pushdown helper
only constructs filters; it does not inspect memo alternatives to merge them.
The projection rule owns its expression substitution and inlining checks, which
are also used by enforcement analysis to recognize predicates below projections.

Aggregation uses the same enforcement checks for predicates inferred on grouping
keys. A retained predicate involving aggregate outputs must not keep restoring a
grouping-key filter that projection and join rules have already pushed lower.

Inferred and effective expressions are simplified before comparing them or
inserting a filter, including constant predicates obtained by substituting join
equalities. Empty inputs imply `FALSE`, even when they have no output columns.
Domain containment recognizes stronger existing constraints and compares array
and row constants using SQL value semantics. A rule returns no result when the
remaining filter and its captured source are unchanged.
Inferred and retained predicates use the same canonical comparison form as
expression cleanup, preventing those rules from repeatedly reversing comparisons.

## Pipeline placement

The pipeline invokes these rules after subquery rewriting, projection pushdown,
cross-join elimination, join reordering, and layout selection. The first pass
does not use connector table properties. Later passes may use them, subject to
the `predicate_pushdown_use_table_properties` session setting.

Ordering boundaries remain necessary where an optimizer consumes a completed
earlier phase. In particular, predicate pushdown finishes before column pruning:
`PushFilterThroughProject` and `PruneFilterColumns` can reverse each other's
movement of a filter and a pruning projection. Both finish before cost-based
join reordering, and predicate pushdown runs again on its result.
Within each combined stage, expression simplification and any connector
predicate pushdown run to a fixed point with relational pushdown. Stages are not
combined across join reordering, layout selection, or dynamic-filter cleanup.

The final relational pass creates dynamic filters after join planning and
inequality-expression projection. Producer identities are retained across rule
applications so consumers are not repeatedly inserted. The existing dynamic
filter cleanup and task-retry source rules run after propagation. Dynamic
filtering can still be disabled independently of static predicate pushdown.

## Validation

Both rule-level tests and complete optimizer tests are necessary. A rule may
produce a correct single transformation while failing to converge with other
rules. Shared planner and query-result suites exercise both implementations,
including dynamic filtering enabled and disabled. Tests cover memo replacements,
unchanged inferred predicates, projection substitution, nested regular and
spatial joins, aggregation barriers,
nondeterminism, failing expressions, and static versus dynamic filtering.
Plan assertions check that filters reach their intended inputs; query-result
tests check evaluation semantics.
