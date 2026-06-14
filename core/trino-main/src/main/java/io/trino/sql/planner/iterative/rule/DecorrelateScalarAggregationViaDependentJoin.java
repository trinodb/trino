/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.trino.sql.planner.iterative.rule;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import io.trino.sql.PlannerContext;
import io.trino.sql.ir.Constant;
import io.trino.sql.ir.Expression;
import io.trino.sql.ir.Logical;
import io.trino.sql.ir.Reference;
import io.trino.sql.planner.NodeAndMappings;
import io.trino.sql.planner.PlanNodeIdAllocator;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.SymbolAllocator;
import io.trino.sql.planner.SymbolsExtractor;
import io.trino.sql.planner.iterative.Lookup;
import io.trino.sql.planner.optimizations.decorrelation.DependentJoinAlgebra;
import io.trino.sql.planner.plan.AggregationNode;
import io.trino.sql.planner.plan.AssignUniqueId;
import io.trino.sql.planner.plan.Assignments;
import io.trino.sql.planner.plan.CorrelatedJoinNode;
import io.trino.sql.planner.plan.FilterNode;
import io.trino.sql.planner.plan.JoinNode;
import io.trino.sql.planner.plan.JoinType;
import io.trino.sql.planner.plan.LimitNode;
import io.trino.sql.planner.plan.PlanNode;
import io.trino.sql.planner.plan.ProjectNode;
import io.trino.sql.planner.plan.TopNNode;
import io.trino.sql.planner.plan.ValuesNode;
import io.trino.type.TypeCoercion;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;

import static com.google.common.collect.Iterables.getOnlyElement;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.sql.ir.Booleans.TRUE;
import static io.trino.sql.ir.IrUtils.and;
import static io.trino.sql.ir.IrUtils.extractConjuncts;
import static io.trino.sql.planner.iterative.rule.AggregationDecorrelation.isNullRowInsensitiveAggregation;
import static io.trino.sql.planner.optimizations.decorrelation.CorrelatedSubqueryShapes.isDistinctOperator;
import static io.trino.sql.planner.optimizations.decorrelation.CorrelatedSubqueryShapes.isExistenceAggregation;
import static io.trino.sql.planner.optimizations.decorrelation.CorrelatedSubqueryShapes.isScalarGlobalAggregation;
import static io.trino.sql.planner.optimizations.decorrelation.CorrelatedSubqueryShapes.isUnnestWithGlobalAggregationShape;
import static io.trino.sql.planner.optimizations.decorrelation.DependentJoinAlgebra.correlationIdentical;
import static io.trino.sql.planner.optimizations.decorrelation.DependentJoinAlgebra.correlationMapping;
import static io.trino.sql.planner.optimizations.decorrelation.DependentJoinAlgebra.reapplyPassingConditionInputs;
import static io.trino.sql.planner.optimizations.decorrelation.GroupConstantAnalysis.groupConstantSymbols;
import static io.trino.sql.planner.plan.AggregationNode.singleAggregation;
import static io.trino.sql.planner.plan.AggregationNode.singleGroupingSet;
import static io.trino.sql.planner.plan.Patterns.CorrelatedJoin.correlation;
import static io.trino.sql.planner.plan.Patterns.correlatedJoin;

/// A scalar global aggregate `(SELECT count(*) …)` (aggregation at the top) needs the
/// non_null-mask treatment: a naive Γ pushdown groups by the correlation and drops the
/// empty group, losing the aggregate-over-empty value (count → 0). A LEFT correlated join
/// over a scalar global aggregate is equivalent to INNER (a global aggregation produces
/// exactly one row), so both types take this path; LEFT requires a TRUE join filter (a
/// non-trivial LEFT filter must null-extend rather than drop, which this path does not do).
public class DecorrelateScalarAggregationViaDependentJoin
        extends DependentJoinFrameworkRule
{
    public DecorrelateScalarAggregationViaDependentJoin(PlannerContext plannerContext)
    {
        super(plannerContext);
    }

    @Override
    protected Optional<PlanNode> apply(CorrelatedJoinNode node, DependentJoinAlgebra algebra, Context context)
    {
        return new Rewriter(plannerContext(), context, algebra).decorrelate(node);
    }

    private static final class Rewriter
    {
        private final PlanNodeIdAllocator idAllocator;
        private final SymbolAllocator symbolAllocator;
        private final Lookup lookup;
        private final TypeCoercion typeCoercion;
        private final DependentJoinAlgebra algebra;

        private Rewriter(PlannerContext plannerContext, Context context, DependentJoinAlgebra algebra)
        {
            this.idAllocator = context.getIdAllocator();
            this.symbolAllocator = context.getSymbolAllocator();
            this.lookup = context.getLookup();
            this.typeCoercion = new TypeCoercion(plannerContext.getTypeManager()::getType);
            this.algebra = algebra;
        }

        private Optional<PlanNode> decorrelate(CorrelatedJoinNode correlatedJoin)
        {
            PlanNode subquery = correlatedJoin.getSubquery();
            if (!isScalarGlobalAggregation(subquery, lookup)
                    || (correlatedJoin.getType() != JoinType.INNER && !correlatedJoin.getFilter().equals(TRUE))) {
                return Optional.empty();
            }
            return decorrelateGlobalAggregation(correlatedJoin, correlatedJoin.getInput(), correlatedJoin.getCorrelation(), correlatedJoin.getFilter())
                    .map(plan -> algebra.restrictOutputs(plan, correlatedJoin.getOutputSymbols()));
        }

        /// Scalar global aggregate `(SELECT count(*) … WHERE correlated)`. Decorrelates the
        /// aggregation's source per distinct correlation value, LEFT-joins the (uniquely tagged) input
        /// back on the correlation columns with a `non_null` marker, then re-aggregates grouped
        /// by the input identity with each aggregate masked by `non_null` — so an outer row whose
        /// subquery is empty becomes a single null-extended row that the mask folds into the correct
        /// aggregate-over-empty value (count → 0, sum → NULL, …). Mirrors
        /// `TransformCorrelatedGlobalAggregationWithoutProjection` but uses the framework pushdown
        /// for the source, so correlation in the source's projections/filters is handled. The existence
        /// aggregate (a single `bool_or(reference)`, what EXISTS lowers to) takes an unmasked
        /// variant — see [#joinThenAggregateSource] — whose output the post-decorrelation
        /// cleanups simplify to the legacy INNER join + dedup plan in filter context. Serves both INNER
        /// and LEFT correlated joins: a global aggregation produces exactly one row, so the two are
        /// equivalent (LEFT is gated on a TRUE correlated-join filter by the caller).
        ///
        /// Leading projections over the aggregate (e.g. `(SELECT count(*) + 1 …)`, or the
        /// existence projection an `EXISTS` adds) are stripped, the aggregate is decorrelated, and
        /// the projections are re-applied on top — they reference the aggregate outputs (preserved) and
        /// may reference correlation (the outer columns, which survive as the re-aggregation's grouping
        /// keys). Mirrors `TransformCorrelatedGlobalAggregationWithProjection`.
        ///
        /// An aggregate that already carries a mask (e.g. a `FILTER` clause) is combined with the
        /// `non_null` marker. A non-trivial correlated-join filter (only possible for an explicit
        /// `JOIN LATERAL … ON …`, since this path is INNER-only) is applied as a filter on top of
        /// the per-outer-row result.
        private Optional<PlanNode> decorrelateGlobalAggregation(CorrelatedJoinNode correlatedJoin, PlanNode input, List<Symbol> correlation, Expression joinFilter)
        {
            // Strip leading projections over the aggregate; they are re-applied after decorrelation.
            List<ProjectNode> projections = new ArrayList<>();
            PlanNode resolved = lookup.resolve(correlatedJoin.getSubquery());
            while (resolved instanceof ProjectNode project) {
                projections.add(project);
                resolved = lookup.resolve(project.getSource());
            }
            if (!(resolved instanceof AggregationNode aggregation)
                    || !aggregation.hasEmptyGroupingSet()
                    || aggregation.getGroupingSetCount() != 1
                    || aggregation.getStep() != AggregationNode.Step.SINGLE) {
                return Optional.empty();
            }
            PlanNode source = aggregation.getSource();

            // An aggregate whose base (below liftable filters/projections) is a VALUES relation that embeds
            // the correlation in its rows runs over the correlation value itself — e.g. `(VALUES k)` as
            // produced by a correlated quantified comparison like `x > ALL (VALUES k)`. Decorrelating
            // it yields a malformed distinct aggregation that a later optimizer rule rejects, so
            // decline — the shape fails as unsupported (legacy also rejects it). (A correlated
            // UNNEST base consumes the correlation as its array input and is handled by
            // visitUnnest, so it is not matched here.)
            PlanNode base = lookup.resolve(source);
            while (true) {
                if (base instanceof ProjectNode project) {
                    base = lookup.resolve(project.getSource());
                }
                else if (base instanceof FilterNode filter) {
                    base = lookup.resolve(filter.getSource());
                }
                else {
                    break;
                }
            }
            if (base instanceof ValuesNode
                    && !Sets.intersection(SymbolsExtractor.extractUnique(base, lookup), ImmutableSet.copyOf(correlation)).isEmpty()) {
                return Optional.empty();
            }

            // An aggregate over an UNNEST of the correlation is the shape the shape-local
            // Decorrelate*UnnestWithGlobalAggregation rules turn into a single join-free unnest,
            // preserving element order for order-sensitive aggregates (array_agg). The framework's
            // magic-set would re-derive it with a distinct set and a join-back, losing both. Defer.
            // A correlated filter around the unnest (which legacy cannot decorrelate) stays here.
            if (isUnnestWithGlobalAggregationShape(source, ImmutableSet.copyOf(correlation), lookup)) {
                return Optional.empty();
            }

            // Tag the input so the re-aggregation can group per outer row even with duplicate rows.
            Symbol unique = symbolAllocator.newSymbol("unique", BIGINT);
            PlanNode taggedInput = new AssignUniqueId(idAllocator.getNextId(), input, unique);

            // The all-equality existence shape (a single unmasked bool_or(reference), what EXISTS
            // lowers to) skips the re-aggregation entirely — see tryExistenceSemiJoin.
            if (isExistenceAggregation(aggregation)) {
                Optional<PlanNode> semiJoin = tryExistenceSemiJoin(aggregation, input, ImmutableSet.copyOf(correlation));
                if (semiJoin.isPresent()) {
                    return Optional.of(algebra.reapplyProjections(semiJoin.get(), projections, input, joinFilter));
                }
            }
            // An aggregation that ignores an all-NULL row (every argument a reference to the
            // null-supplying side, every function skipping NULL inputs — the same
            // isNullRowInsensitiveAggregation test the legacy rules use to skip the mask) needs NO
            // non_null marker: the LEFT join's null-extended row contributes nothing, so empty groups
            // come out right unmasked. Emitting the unmasked form matters for plan quality — for the
            // existence aggregate it is the shape the post-decorrelation cleanups match
            // (PushFilterThroughBoolOrAggregation, then LEFT-to-INNER conversion), which turn a
            // filter-context EXISTS into the same INNER join + dedup the legacy decorrelator reaches.
            // Requires the source's projections to be correlation-free so they can stay below the
            // join. Dedup placement is exempted from the group-constant rule only for the
            // duplicate-insensitive existence aggregate, not for unmasked aggregations generally.
            boolean masked = true;
            Optional<PlanNode> joinedOptional = Optional.empty();
            if (isNullRowInsensitiveAggregation(aggregation)) {
                joinedOptional = joinThenAggregateSource(taggedInput, source, ImmutableSet.copyOf(correlation), Optional.empty(), isExistenceAggregation(aggregation));
                masked = joinedOptional.isEmpty();
            }

            // Otherwise prefer the legacy-equivalent join-then-aggregate (a single LEFT join, no
            // distinct/clone) when the correlation is filter-only over a correlation-free base, with a
            // non_null marker on the join's right side masking every aggregate; fall back to the
            // magic-set (distinct-D + NULL-safe join-back) for deeper correlation.
            Symbol nonNull = symbolAllocator.newSymbol("non_null", BOOLEAN);
            if (joinedOptional.isEmpty()) {
                joinedOptional = joinThenAggregateSource(taggedInput, source, ImmutableSet.copyOf(correlation), Optional.of(nonNull), false);
            }
            if (joinedOptional.isEmpty()) {
                joinedOptional = magicSetAggregateSource(input, taggedInput, source, correlation, nonNull);
            }
            if (joinedOptional.isEmpty()) {
                return Optional.empty();
            }
            PlanNode joined = joinedOptional.get();

            // Re-aggregate per outer row. On the masked path each aggregate is masked by non_null to
            // restore empty groups; an aggregate that already carries a mask (e.g. a FILTER clause,
            // which may reference correlation) gets `mask AND non_null` instead, so the null-extended
            // row of an empty group is still excluded. The null-insensitive path re-applies the
            // original aggregations unchanged.
            Map<Symbol, AggregationNode.Aggregation> reaggregations;
            PlanNode reaggregationSource = joined;
            if (masked) {
                Map<Symbol, Symbol> combinedMask = new HashMap<>();
                Assignments.Builder maskAssignments = Assignments.builder().putIdentities(joined.getOutputSymbols());
                aggregation.getAggregations().values().forEach(aggregationEntry -> aggregationEntry.getMask().ifPresent(mask ->
                        combinedMask.computeIfAbsent(mask, existing -> {
                            Symbol combined = symbolAllocator.newSymbol("mask", BOOLEAN);
                            maskAssignments.put(combined, new Logical(Logical.Operator.AND, ImmutableList.of(existing.toSymbolReference(), nonNull.toSymbolReference())));
                            return combined;
                        })));
                if (!combinedMask.isEmpty()) {
                    reaggregationSource = new ProjectNode(idAllocator.getNextId(), joined, maskAssignments.build());
                }

                ImmutableMap.Builder<Symbol, AggregationNode.Aggregation> maskedAggregations = ImmutableMap.builder();
                aggregation.getAggregations().forEach((symbol, aggregationEntry) -> maskedAggregations.put(symbol, new AggregationNode.Aggregation(
                        aggregationEntry.getResolvedFunction(),
                        aggregationEntry.getArguments(),
                        aggregationEntry.isDistinct(),
                        aggregationEntry.getFilter(),
                        aggregationEntry.getOrderingScheme(),
                        Optional.of(aggregationEntry.getMask().map(combinedMask::get).orElse(nonNull)))));
                reaggregations = maskedAggregations.buildOrThrow();
            }
            else {
                reaggregations = aggregation.getAggregations();
            }
            PlanNode reaggregated = new AggregationNode(
                    idAllocator.getNextId(),
                    reaggregationSource,
                    reaggregations,
                    singleGroupingSet(taggedInput.getOutputSymbols()),
                    ImmutableList.of(),
                    AggregationNode.Step.SINGLE,
                    Optional.empty(),
                    Optional.empty());

            return Optional.of(algebra.reapplyProjections(reaggregated, projections, input, joinFilter));
        }

        /// All-equality filter-only existence — the semi-join form. When the existence aggregate's
        /// argument is a constant projected on the build side, and every base column the correlated
        /// predicates use is pinned by an equality to the input, the build side dedup'd on those
        /// columns yields at most one match per input row — so no re-aggregation is needed at all:
        /// the `bool_or` value IS the null-extended marker of the single match. This is exactly
        /// the plan legacy's marker-over-Limit path reaches (a LEFT join to a distinct build +
        /// COALESCE), which the cleanups turn into a plain INNER join in filter context.
        ///
        /// Existence is insensitive to positive row bounds and to dedups, so those are dropped from
        /// the chain. One grouped aggregation (a `GROUP BY … HAVING` subquery) may stay below
        /// the join, provided the correlated predicates sit below it and reference only its grouping
        /// keys — the uncorrelated HAVING conjuncts filter the groups in place, and a dedup on the
        /// pinned columns restores the at-most-one-match guarantee when they don't cover the keys.
        /// Uncorrelated conjuncts stay below in their original position. Returns empty when the shape
        /// or the equality-pinning requirement doesn't hold.
        private Optional<PlanNode> tryExistenceSemiJoin(AggregationNode aggregation, PlanNode input, Set<Symbol> correlation)
        {
            Symbol boolSymbol = getOnlyElement(aggregation.getAggregations().keySet());
            // isExistenceAggregation (checked by the caller) guarantees a single bool_or(reference)
            Symbol argument = Symbol.from((Reference) getOnlyElement(aggregation.getAggregations().values()).getArguments().getFirst());

            List<ProjectNode> projections = new ArrayList<>();
            List<Expression> correlatedPredicates = new ArrayList<>();
            List<Expression> uncorrelatedAbove = new ArrayList<>();
            List<Expression> uncorrelatedBelow = new ArrayList<>();
            AggregationNode groupedOperator = null;
            boolean correlatedAboveOperator = false;
            PlanNode node = lookup.resolve(aggregation.getSource());
            while (true) {
                if (node instanceof ProjectNode project
                        && groupedOperator == null
                        && correlatedPredicates.isEmpty() && uncorrelatedAbove.isEmpty()) {
                    projections.add(project);
                    node = lookup.resolve(project.getSource());
                }
                else if (node instanceof ProjectNode project && project.getAssignments().isIdentity()) {
                    // A pure pruning projection is transparent: nothing of it survives the rewrite.
                    node = lookup.resolve(project.getSource());
                }
                else if (groupedOperator == null
                        && correlatedPredicates.isEmpty() && uncorrelatedAbove.isEmpty()
                        && (node instanceof LimitNode limit && limit.getCount() >= 1
                        || node instanceof TopNNode topN && topN.getCount() >= 1)) {
                    // Existence is insensitive to a positive row bound (the marker is a constant,
                    // checked below) — but only relative to what sits BELOW the bound: for filters
                    // already collected above it, "some row of the top-k passes" is not "some row
                    // passes". Drop the bound only while no filter conjuncts have been collected
                    // (the same guard as the projection arm).
                    node = lookup.resolve(node.getSources().getFirst());
                }
                else if (node instanceof AggregationNode distinct && groupedOperator == null && isDistinctOperator(distinct)) {
                    // Existence is insensitive to deduplication (a GROUP BY with no aggregates) too
                    // — and unlike the bound-drop, this needs no collected-filter guard: a dedup
                    // outputs exactly its keys and preserves the set of key tuples, so a predicate
                    // over them passes for some row of the dedup iff it passes for some source row.
                    node = lookup.resolve(distinct.getSource());
                }
                else if (node instanceof AggregationNode grouped
                        && groupedOperator == null
                        && grouped.getStep() == AggregationNode.Step.SINGLE
                        && grouped.getGroupingSetCount() == 1
                        && grouped.hasNonEmptyGroupingSet()
                        && Sets.intersection(SymbolsExtractor.extractUniqueNonRecursive(grouped), correlation).isEmpty()) {
                    // A GROUP BY … HAVING subquery: the aggregation stays below the join.
                    groupedOperator = grouped;
                    if (!correlatedPredicates.isEmpty()) {
                        correlatedAboveOperator = true;
                    }
                    node = lookup.resolve(grouped.getSource());
                }
                else if (node instanceof FilterNode filter) {
                    for (Expression conjunct : extractConjuncts(filter.getPredicate())) {
                        boolean correlated = !Sets.intersection(SymbolsExtractor.extractUnique(conjunct), correlation).isEmpty();
                        if (correlated) {
                            correlatedPredicates.add(conjunct);
                        }
                        else {
                            (groupedOperator == null ? uncorrelatedAbove : uncorrelatedBelow).add(conjunct);
                        }
                    }
                    node = lookup.resolve(filter.getSource());
                }
                else {
                    break;
                }
            }
            PlanNode base = node;
            if (!Sets.intersection(SymbolsExtractor.extractUnique(base, lookup), correlation).isEmpty()) {
                return Optional.empty();
            }
            // A correlated predicate above the grouped aggregation references its results, which the
            // semi-join form cannot pin; uncorrelated-above conjuncts (HAVING) need the aggregation in
            // place to be meaningful.
            if (groupedOperator == null) {
                uncorrelatedBelow.addAll(uncorrelatedAbove);
                uncorrelatedAbove.clear();
            }
            else if (correlatedAboveOperator) {
                return Optional.empty();
            }
            // The aggregate's argument must be a constant marker projected on the build side (the
            // existence shape: subqueryTrue := TRUE) — that is what makes dropping bounds and dedups
            // sound, and what lets the null-extended marker stand in for the bool_or value. The other
            // projections (e.g. pruning identities) are discarded: only the marker and the condition
            // columns survive above the dedup.
            Expression argumentExpression = projections.stream()
                    .map(projection -> projection.getAssignments().get(argument))
                    .filter(Objects::nonNull)
                    .findFirst()
                    .orElse(null);
            if (!(argumentExpression instanceof Constant)) {
                return Optional.empty();
            }
            if (correlatedPredicates.isEmpty()) {
                return Optional.empty();
            }

            Expression condition = and(correlatedPredicates);
            Set<Symbol> conditionInputs = SymbolsExtractor.extractUnique(condition);

            PlanNode build = base;
            if (!uncorrelatedBelow.isEmpty()) {
                build = new FilterNode(
                        idAllocator.getNextId(),
                        build,
                        and(uncorrelatedBelow));
            }
            if (groupedOperator != null) {
                // The correlated predicates must reference only the grouping keys (the keys cannot be
                // extended: that would change the groups and any HAVING semantics).
                if (!ImmutableSet.copyOf(groupedOperator.getGroupingKeys())
                        .containsAll(Sets.intersection(conditionInputs, ImmutableSet.copyOf(build.getOutputSymbols())))) {
                    return Optional.empty();
                }
                build = new AggregationNode(
                        idAllocator.getNextId(),
                        build,
                        groupedOperator.getAggregations(),
                        singleGroupingSet(groupedOperator.getGroupingKeys()),
                        ImmutableList.of(),
                        AggregationNode.Step.SINGLE,
                        Optional.empty(),
                        Optional.empty());
                if (!uncorrelatedAbove.isEmpty()) {
                    build = new FilterNode(
                            idAllocator.getNextId(),
                            build,
                            and(uncorrelatedAbove));
                }
            }
            ImmutableList.Builder<Symbol> dedupKeysBuilder = ImmutableList.builder();
            for (Symbol symbol : build.getOutputSymbols()) {
                if (conditionInputs.contains(symbol)) {
                    dedupKeysBuilder.add(symbol);
                }
            }
            List<Symbol> dedupKeys = dedupKeysBuilder.build();
            // Every condition base column must be equality-pinned to the input — then a dedup'd build
            // yields at most one match per input row. (Empty keys must not dedup: a global aggregation
            // emits a row even over empty input, turning an empty subquery into a match.)
            if (dedupKeys.isEmpty()
                    || !groupConstantSymbols(condition, ImmutableSet.copyOf(build.getOutputSymbols()), typeCoercion).containsAll(dedupKeys)) {
                return Optional.empty();
            }
            // A grouped operator whose keys are exactly the pinned columns is already unique on them.
            if (groupedOperator == null || !ImmutableSet.copyOf(groupedOperator.getGroupingKeys()).equals(ImmutableSet.copyOf(dedupKeys))) {
                build = new AggregationNode(
                        idAllocator.getNextId(),
                        build,
                        ImmutableMap.of(),
                        singleGroupingSet(dedupKeys),
                        ImmutableList.of(),
                        AggregationNode.Step.SINGLE,
                        Optional.empty(),
                        Optional.empty());
            }
            // Only the constant marker and the condition columns survive above the dedup; the original
            // projections (which may reference dedup'd-away columns) are not needed.
            build = new ProjectNode(
                    idAllocator.getNextId(),
                    build,
                    Assignments.builder()
                            .putIdentities(ImmutableSet.copyOf(dedupKeys))
                            .put(argument, argumentExpression)
                            .build());

            PlanNode joined = new JoinNode(
                    idAllocator.getNextId(),
                    JoinType.LEFT,
                    input,
                    build,
                    ImmutableList.of(),
                    input.getOutputSymbols(),
                    build.getOutputSymbols(),
                    false,
                    Optional.of(condition),
                    Optional.empty(),
                    Optional.empty(),
                    ImmutableMap.of(),
                    Optional.empty());

            // The aggregate's value is the (null-extended) marker of the at-most-one match.
            return Optional.of(new ProjectNode(
                    idAllocator.getNextId(),
                    joined,
                    Assignments.builder()
                            .putIdentities(input.getOutputSymbols())
                            .put(boolSymbol, argument.toSymbolReference())
                            .build()));
        }

        /// Join-then-aggregate decorrelation of a scalar global aggregate's source — the cheap form
        /// (one LEFT join) that legacy `TransformCorrelatedGlobalAggregation*` produces. Applies
        /// when the source is `π*(σ*(B))` with `B` correlation-free: LEFT-join the
        /// (uniquely tagged) input to `B` on the lifted filter predicates. An input row with no
        /// match null-extends, so the empty group folds to the aggregate-over-empty value.
        ///
        /// With a `nonNull` marker (the general, masked form), `B` is marked with
        /// `non_null := TRUE` and the source's projections (which compute the aggregate
        /// arguments, possibly from correlation) are re-applied above the join; the caller masks every
        /// aggregate by `non_null`. Without a marker (the null-insensitive existence form), the
        /// source's projections must be correlation-free and stay below the join on the null-supplying
        /// side, so the aggregate arguments themselves null-extend — no mask, which keeps the
        /// aggregation matchable by the post-decorrelation cleanups
        /// (`PushFilterThroughBoolOrAggregation`). Returns empty when the correlation is deeper than
        /// liftable filters/projections (leaving the magic-set to handle it), or — in the unmarked
        /// form — when a projection references correlation.
        ///
        /// Both forms also accept a distinct operator (a pure dedup aggregation, planned for
        /// `count(DISTINCT …)` and for `GROUP BY`-shaped existence subqueries) between the
        /// projections and the filters — one join instead of the magic-set's clone + distinct +
        /// join-back. When the source's projections are correlation-free, the whole source — dedup
        /// included — stays below the join (mirroring legacy's `PlanNodeDecorrelator`): the
        /// dedup's grouping keys are extended with the base columns the lifted predicates use, which
        /// makes filtering above it equivalent to filtering below it, and leaves the re-aggregation
        /// sitting directly on the join where `PushAggregationThroughOuterJoin` can push it to
        /// the build side. Correlated projections (masked form only, beyond what legacy handles) are
        /// re-applied above the join instead, with the dedup restored above it too — the input columns,
        /// the unique id, and the marker joining its grouping keys.
        private Optional<PlanNode> joinThenAggregateSource(PlanNode taggedInput, PlanNode source, Set<Symbol> correlation, Optional<Symbol> nonNull, boolean duplicateInsensitive)
        {
            List<ProjectNode> projectionsAboveDistinct = new ArrayList<>();
            List<ProjectNode> projectionsBelowDistinct = new ArrayList<>();
            List<Expression> predicates = new ArrayList<>();
            AggregationNode distinctOperator = null;
            PlanNode node = lookup.resolve(source);
            while (true) {
                if (node instanceof ProjectNode project && predicates.isEmpty()) {
                    (distinctOperator == null ? projectionsAboveDistinct : projectionsBelowDistinct).add(project);
                    node = lookup.resolve(project.getSource());
                }
                else if (node instanceof AggregationNode aggregation
                        && distinctOperator == null
                        && predicates.isEmpty()
                        && isDistinctOperator(aggregation)) {
                    distinctOperator = aggregation;
                    node = lookup.resolve(aggregation.getSource());
                }
                else if (node instanceof FilterNode filter) {
                    predicates.add(filter.getPredicate());
                    node = lookup.resolve(filter.getSource());
                }
                else {
                    break;
                }
            }
            PlanNode base = node;
            if (!Sets.intersection(SymbolsExtractor.extractUnique(base, lookup), correlation).isEmpty()) {
                return Optional.empty();
            }
            boolean projectionsCorrelationFree = Stream.concat(projectionsAboveDistinct.stream(), projectionsBelowDistinct.stream())
                    .flatMap(projection -> projection.getAssignments().expressions().stream())
                    .allMatch(expression -> Sets.intersection(SymbolsExtractor.extractUnique(expression), correlation).isEmpty());
            if (nonNull.isEmpty() && !projectionsCorrelationFree) {
                return Optional.empty();
            }

            Expression condition = and(predicates);
            Set<Symbol> conditionInputs = SymbolsExtractor.extractUnique(condition);

            // The parity form keeps the whole source — dedup included — below the join. The dedup's
            // keys must be extended with the condition's base columns (so filtering above it equals
            // filtering below it), which for a duplicate-SENSITIVE re-aggregation is sound only when
            // every extension column is group-constant — equated to the input side by an equality
            // conjunct (legacy's constant-symbol rule). A duplicate-insensitive bool_or (the unmasked
            // existence form) tolerates the finer grouping for any predicate. Otherwise the dedup must
            // apply per outer row, above the join.
            boolean belowJoinForm = projectionsCorrelationFree;
            PlanNode builtBelow = null;
            if (belowJoinForm) {
                builtBelow = reapplyPassingConditionInputs(idAllocator, base, projectionsBelowDistinct, conditionInputs);
                // The existence form — unmasked bool_or with every projected value a constant — needs
                // only the condition's base columns from the build side, and bool_or is
                // duplicate-insensitive, so dedup the build side on them when they are group-constant.
                // This mirrors the build-side distinct legacy's Limit(1) handling produces for
                // equality-correlated EXISTS. (Empty keys must not dedup: a global aggregation emits a
                // row even over empty input, which would turn an empty subquery into a match.)
                if (distinctOperator == null
                        && duplicateInsensitive
                        && Stream.concat(projectionsAboveDistinct.stream(), projectionsBelowDistinct.stream())
                        .flatMap(projection -> projection.getAssignments().expressions().stream())
                        .allMatch(Constant.class::isInstance)) {
                    ImmutableList.Builder<Symbol> dedupKeysBuilder = ImmutableList.builder();
                    for (Symbol symbol : builtBelow.getOutputSymbols()) {
                        if (conditionInputs.contains(symbol)) {
                            dedupKeysBuilder.add(symbol);
                        }
                    }
                    List<Symbol> dedupKeys = dedupKeysBuilder.build();
                    if (!dedupKeys.isEmpty()
                            && groupConstantSymbols(condition, ImmutableSet.copyOf(builtBelow.getOutputSymbols()), typeCoercion).containsAll(dedupKeys)) {
                        builtBelow = new AggregationNode(
                                idAllocator.getNextId(),
                                builtBelow,
                                ImmutableMap.of(),
                                singleGroupingSet(dedupKeys),
                                ImmutableList.of(),
                                AggregationNode.Step.SINGLE,
                                Optional.empty(),
                                Optional.empty());
                    }
                }
                if (distinctOperator != null) {
                    ImmutableList.Builder<Symbol> dedupKeysBuilder = ImmutableList.<Symbol>builder()
                            .addAll(distinctOperator.getGroupingKeys());
                    ImmutableSet.Builder<Symbol> extensionsBuilder = ImmutableSet.builder();
                    for (Symbol symbol : builtBelow.getOutputSymbols()) {
                        if (conditionInputs.contains(symbol) && !distinctOperator.getGroupingKeys().contains(symbol)) {
                            dedupKeysBuilder.add(symbol);
                            extensionsBuilder.add(symbol);
                        }
                    }
                    Set<Symbol> extensions = extensionsBuilder.build();
                    if (!duplicateInsensitive
                            && !groupConstantSymbols(condition, ImmutableSet.copyOf(builtBelow.getOutputSymbols()), typeCoercion).containsAll(extensions)) {
                        belowJoinForm = false;
                    }
                    else {
                        builtBelow = new AggregationNode(
                                idAllocator.getNextId(),
                                builtBelow,
                                ImmutableMap.of(),
                                singleGroupingSet(dedupKeysBuilder.build()),
                                ImmutableList.of(),
                                AggregationNode.Step.SINGLE,
                                Optional.empty(),
                                Optional.empty());
                    }
                }
            }

            // The above-join form restores the dedup with the non_null marker in its keys, so an
            // unmasked invocation cannot take it — decline and let the caller retry with the mask.
            if (!belowJoinForm && nonNull.isEmpty()) {
                return Optional.empty();
            }

            PlanNode rightSide;
            if (belowJoinForm) {
                rightSide = reapplyPassingConditionInputs(idAllocator, builtBelow, projectionsAboveDistinct, conditionInputs);
                if (nonNull.isPresent()) {
                    rightSide = new ProjectNode(
                            idAllocator.getNextId(),
                            rightSide,
                            Assignments.builder().putIdentities(rightSide.getOutputSymbols()).put(nonNull.orElseThrow(), TRUE).build());
                }
            }
            else {
                rightSide = new ProjectNode(
                        idAllocator.getNextId(),
                        base,
                        Assignments.builder().putIdentities(base.getOutputSymbols()).put(nonNull.orElseThrow(), TRUE).build());
            }

            PlanNode result = new JoinNode(
                    idAllocator.getNextId(),
                    JoinType.LEFT,
                    taggedInput,
                    rightSide,
                    ImmutableList.of(),
                    taggedInput.getOutputSymbols(),
                    rightSide.getOutputSymbols(),
                    false,
                    condition.equals(TRUE) ? Optional.empty() : Optional.of(condition),
                    Optional.empty(),
                    Optional.empty(),
                    ImmutableMap.of(),
                    Optional.empty());

            if (!belowJoinForm) {
                // Correlated projections need the input columns in scope, and a dedup whose key
                // extension would not be group-constant must apply per outer row — so the source is
                // re-applied above the join: the below-dedup projections, the dedup — with the input
                // columns, the unique id, and the non_null marker joining its grouping keys, so
                // null-extended rows of empty groups survive — and the above-dedup projections; all
                // passing the input columns and the marker through for the re-aggregation to mask on.
                result = reapplyPassingInputAndMarker(result, projectionsBelowDistinct, taggedInput, nonNull.orElseThrow());
                if (distinctOperator != null) {
                    List<Symbol> dedupKeys = ImmutableList.<Symbol>builder()
                            .addAll(taggedInput.getOutputSymbols())
                            .addAll(distinctOperator.getGroupingKeys())
                            .add(nonNull.orElseThrow())
                            .build();
                    result = new AggregationNode(
                            idAllocator.getNextId(),
                            result,
                            ImmutableMap.of(),
                            singleGroupingSet(dedupKeys),
                            ImmutableList.of(),
                            AggregationNode.Step.SINGLE,
                            Optional.empty(),
                            Optional.empty());
                }
                result = reapplyPassingInputAndMarker(result, projectionsAboveDistinct, taggedInput, nonNull.orElseThrow());
            }
            return Optional.of(result);
        }

        private PlanNode reapplyPassingInputAndMarker(PlanNode source, List<ProjectNode> projections, PlanNode taggedInput, Symbol nonNull)
        {
            PlanNode result = source;
            for (int i = projections.size() - 1; i >= 0; i--) {
                Assignments assignments = projections.get(i).getAssignments();
                Assignments.Builder augmented = Assignments.builder().putAll(assignments);
                for (Symbol symbol : taggedInput.getOutputSymbols()) {
                    if (!assignments.outputs().contains(symbol)) {
                        augmented.putIdentity(symbol);
                    }
                }
                if (!assignments.outputs().contains(nonNull)) {
                    augmented.putIdentity(nonNull);
                }
                result = new ProjectNode(idAllocator.getNextId(), result, augmented.build());
            }
            return result;
        }

        /// Magic-set decorrelation of a scalar global aggregate's source (fallback when the correlation
        /// is deeper than liftable filters): evaluate the source once per distinct correlation value
        /// (clone the input, build `D = δ(π_{C'`)}, push the rebound source through), mark the rows,
        /// and LEFT-join the (uniquely tagged) input back on the correlation columns (NULL-safe).
        private Optional<PlanNode> magicSetAggregateSource(PlanNode input, PlanNode taggedInput, PlanNode source, List<Symbol> correlation, Symbol nonNull)
        {
            Optional<NodeAndMappings> cloneOptional = algebra.copyInput(input, correlation);
            if (cloneOptional.isEmpty()) {
                return Optional.empty();
            }
            NodeAndMappings clone = cloneOptional.get();
            List<Symbol> clonedCorrelation = clone.getFields();
            PlanNode distinctCorrelation = singleAggregation(
                    idAllocator.getNextId(),
                    clone.getNode(),
                    ImmutableMap.of(),
                    singleGroupingSet(clonedCorrelation));
            Optional<PlanNode> reboundSource = algebra.rebindCorrelation(source, correlationMapping(correlation, clonedCorrelation));
            if (reboundSource.isEmpty()) {
                return Optional.empty();
            }
            PlanNode perD = algebra.dependentJoin(distinctCorrelation, reboundSource.get(), ImmutableSet.copyOf(clonedCorrelation));
            PlanNode marked = new ProjectNode(
                    idAllocator.getNextId(),
                    perD,
                    Assignments.builder().putIdentities(perD.getOutputSymbols()).put(nonNull, TRUE).build());
            return Optional.of(new JoinNode(
                    idAllocator.getNextId(),
                    JoinType.LEFT,
                    taggedInput,
                    marked,
                    ImmutableList.of(),
                    taggedInput.getOutputSymbols(),
                    ImmutableList.<Symbol>builder().addAll(source.getOutputSymbols()).add(nonNull).build(),
                    false,
                    Optional.of(correlationIdentical(correlation, clonedCorrelation)),
                    Optional.empty(),
                    Optional.empty(),
                    ImmutableMap.of(),
                    Optional.empty()));
        }
    }
}
