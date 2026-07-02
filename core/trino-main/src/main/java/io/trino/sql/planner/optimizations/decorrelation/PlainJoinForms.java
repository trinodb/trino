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

package io.trino.sql.planner.optimizations.decorrelation;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import io.trino.spi.connector.SortOrder;
import io.trino.sql.PlannerContext;
import io.trino.sql.ir.Call;
import io.trino.sql.ir.Cast;
import io.trino.sql.ir.Expression;
import io.trino.sql.ir.IrExpressions.Comparison;
import io.trino.sql.ir.Reference;
import io.trino.sql.planner.OrderingScheme;
import io.trino.sql.planner.PlanNodeIdAllocator;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.SymbolAllocator;
import io.trino.sql.planner.SymbolsExtractor;
import io.trino.sql.planner.iterative.Lookup;
import io.trino.sql.planner.plan.AggregationNode;
import io.trino.sql.planner.plan.AssignUniqueId;
import io.trino.sql.planner.plan.Assignments;
import io.trino.sql.planner.plan.DataOrganizationSpecification;
import io.trino.sql.planner.plan.FilterNode;
import io.trino.sql.planner.plan.JoinNode;
import io.trino.sql.planner.plan.JoinType;
import io.trino.sql.planner.plan.LimitNode;
import io.trino.sql.planner.plan.PlanNode;
import io.trino.sql.planner.plan.ProjectNode;
import io.trino.sql.planner.plan.RowNumberNode;
import io.trino.sql.planner.plan.TopNNode;
import io.trino.sql.planner.plan.TopNRankingNode;
import io.trino.type.TypeCoercion;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;

import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.sql.ir.Booleans.TRUE;
import static io.trino.sql.ir.IrExpressions.matchComparison;
import static io.trino.sql.ir.IrUtils.and;
import static io.trino.sql.ir.IrUtils.extractConjuncts;
import static io.trino.sql.planner.optimizations.decorrelation.CorrelatedSubqueryShapes.isDistinctOperator;
import static io.trino.sql.planner.optimizations.decorrelation.DependentJoinAlgebra.reapplyPassingConditionInputs;
import static io.trino.sql.planner.optimizations.decorrelation.GroupConstantAnalysis.groupConstantSymbols;
import static io.trino.sql.planner.plan.AggregationNode.singleGroupingSet;
import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;

/// The single-join legacy-parity decorrelation forms: a filter-only LIMIT/TopN re-emitted below a
/// plain join when its grouping is equality-correlated, a dedup kept below (or restored above) a
/// LEFT join, a grouped aggregation below a LEFT join with group-constant key extension, and the
/// plain LEFT join for liftable correlation. These beat both the generic pushdown and the
/// magic-set on plan quality (no unique-id window, no input clone), so the rules try them first.
/// Each is decided by a walk of the subquery chain; a non-match returns empty.
public final class PlainJoinForms
{
    private final PlanNodeIdAllocator idAllocator;
    private final SymbolAllocator symbolAllocator;
    private final Lookup lookup;
    private final TypeCoercion typeCoercion;

    public PlainJoinForms(PlanNodeIdAllocator idAllocator, SymbolAllocator symbolAllocator, Lookup lookup, PlannerContext plannerContext)
    {
        this.idAllocator = requireNonNull(idAllocator, "idAllocator is null");
        this.symbolAllocator = requireNonNull(symbolAllocator, "symbolAllocator is null");
        this.lookup = requireNonNull(lookup, "lookup is null");
        requireNonNull(plannerContext, "plannerContext is null");
        this.typeCoercion = new TypeCoercion(plannerContext.getTypeManager()::getType);
    }

    /// `LEFT JOIN LATERAL (SELECT DISTINCT …)` — a distinct operator over liftable filters,
    /// mirroring legacy `TransformCorrelatedDistinctAggregation*`: one LEFT join of the
    /// uniquely-tagged input on the lifted predicates, the dedup re-applied above it keyed by the
    /// input columns, the unique id, and its original keys (the null-extended row of an unmatched
    /// input survives as that row's single group), and the projections re-applied on top. Each
    /// projection must derive from the subquery side so it null-extends for unmatched rows.
    /// Returns empty when the shape doesn't match.
    public Optional<PlanNode> tryPlainLeftJoinDistinct(PlanNode input, PlanNode subquery, Set<Symbol> correlation)
    {
        List<ProjectNode> projections = new ArrayList<>();
        List<Expression> predicates = new ArrayList<>();
        AggregationNode distinctOperator = null;
        PlanNode node = lookup.resolve(subquery);
        while (true) {
            if (node instanceof ProjectNode project && distinctOperator == null && predicates.isEmpty()) {
                projections.add(project);
                node = lookup.resolve(project.getSource());
            }
            else if (node instanceof AggregationNode aggregation
                    && distinctOperator == null
                    && predicates.isEmpty()
                    && isDistinctOperator(aggregation)) {
                distinctOperator = aggregation;
                node = lookup.resolve(aggregation.getSource());
            }
            else if (node instanceof FilterNode filter && distinctOperator != null) {
                predicates.add(filter.getPredicate());
                node = lookup.resolve(filter.getSource());
            }
            else {
                break;
            }
        }
        if (distinctOperator == null) {
            return Optional.empty();
        }
        PlanNode base = node;
        if (!Sets.intersection(SymbolsExtractor.extractUnique(base, lookup), correlation).isEmpty()) {
            return Optional.empty();
        }
        // Projections re-applied above the join must yield NULL for unmatched input rows — same
        // null-on-null requirement (not mere derivation) as tryPlainLeftJoin's lifted projections.
        Set<Symbol> dedupDerived = Sets.newHashSet(distinctOperator.getOutputSymbols());
        for (int i = projections.size() - 1; i >= 0; i--) {
            for (Expression expression : projections.get(i).getAssignments().expressions()) {
                if (!nullOnNull(expression, dedupDerived)) {
                    return Optional.empty();
                }
            }
            dedupDerived.addAll(projections.get(i).getOutputSymbols());
        }

        Expression condition = and(predicates);

        PlanNode taggedInput = new AssignUniqueId(idAllocator.getNextId(), input, symbolAllocator.newSymbol("unique", BIGINT));
        PlanNode joined = new JoinNode(
                idAllocator.getNextId(),
                JoinType.LEFT,
                taggedInput,
                base,
                ImmutableList.of(),
                taggedInput.getOutputSymbols(),
                base.getOutputSymbols(),
                false,
                condition.equals(TRUE) ? Optional.empty() : Optional.of(condition),
                Optional.empty(),
                Optional.empty(),
                ImmutableMap.of(),
                Optional.empty());

        List<Symbol> dedupKeys = ImmutableList.<Symbol>builder()
                .addAll(taggedInput.getOutputSymbols())
                .addAll(distinctOperator.getGroupingKeys())
                .build();
        PlanNode result = new AggregationNode(
                idAllocator.getNextId(),
                joined,
                ImmutableMap.of(),
                singleGroupingSet(dedupKeys),
                ImmutableList.of(),
                AggregationNode.Step.SINGLE,
                Optional.empty(),
                Optional.empty());

        // Re-apply the projections (innermost first), passing the input columns through.
        for (int i = projections.size() - 1; i >= 0; i--) {
            Assignments assignments = projections.get(i).getAssignments();
            Assignments.Builder augmented = Assignments.builder().putAll(assignments);
            for (Symbol symbol : input.getOutputSymbols()) {
                if (!assignments.outputs().contains(symbol)) {
                    augmented.putIdentity(symbol);
                }
            }
            result = new ProjectNode(idAllocator.getNextId(), result, augmented.build());
        }
        return Optional.of(result);
    }

    /// LATERAL `LIMIT`/`TopN` over filter-only correlation — the legacy parity form
    /// (`PlanNodeDecorrelator`'s limit handling): the limit is re-emitted below the join,
    /// grouped per outer row by the equality-correlated base columns (legacy's constant symbols) —
    /// a `RowNumberNode` with a per-partition row cap for `LIMIT`, a
    /// `TopNRankingNode` for an ordered `TopN` (the ordering stripped of the constant
    /// columns), or the plain node when there is nothing to partition by. Correlated conjuncts
    /// become the join condition (applied after the limit, gating whole groups since they only
    /// reference the partition columns and the input); uncorrelated conjuncts stay below the limit.
    /// Beyond legacy, this also covers non-constant `LIMIT 1` subqueries. Returns empty when
    /// the shape doesn't match or a correlated conjunct references a base column that is not
    /// group-constant — the magic-set (or the generic uid pushdown for INNER) handles those.
    public Optional<PlanNode> tryPlainJoinLimit(JoinType joinType, PlanNode input, PlanNode subquery, Set<Symbol> correlation)
    {
        List<ProjectNode> projectionsAboveLimit = new ArrayList<>();
        List<ProjectNode> projectionsBelowLimit = new ArrayList<>();
        List<Expression> correlatedPredicates = new ArrayList<>();
        List<Expression> uncorrelatedPredicates = new ArrayList<>();
        PlanNode limit = null;
        PlanNode node = lookup.resolve(subquery);
        while (true) {
            if (node instanceof ProjectNode project && correlatedPredicates.isEmpty() && uncorrelatedPredicates.isEmpty()) {
                (limit == null ? projectionsAboveLimit : projectionsBelowLimit).add(project);
                node = lookup.resolve(project.getSource());
            }
            else if (limit == null
                    && correlatedPredicates.isEmpty() && uncorrelatedPredicates.isEmpty()
                    && (node instanceof LimitNode limitNode && !limitNode.isWithTies() && limitNode.getCount() > 0
                    || node instanceof TopNNode topN && topN.getCount() > 0)) {
                limit = node;
                node = lookup.resolve(node.getSources().getFirst());
            }
            else if (node instanceof FilterNode filter && limit != null) {
                // A filter above the limit would have to stay above it; only the below-limit shape
                // (the planner's WHERE placement) is handled here.
                for (Expression conjunct : extractConjuncts(filter.getPredicate())) {
                    boolean correlated = !Sets.intersection(SymbolsExtractor.extractUnique(conjunct), correlation).isEmpty();
                    (correlated ? correlatedPredicates : uncorrelatedPredicates).add(conjunct);
                }
                node = lookup.resolve(filter.getSource());
            }
            else {
                break;
            }
        }
        if (limit == null) {
            return Optional.empty();
        }
        PlanNode base = node;
        if (!Sets.intersection(SymbolsExtractor.extractUnique(base, lookup), correlation).isEmpty()) {
            return Optional.empty();
        }
        if (Stream.concat(projectionsAboveLimit.stream(), projectionsBelowLimit.stream())
                .flatMap(projection -> projection.getAssignments().expressions().stream())
                .anyMatch(expression -> !Sets.intersection(SymbolsExtractor.extractUnique(expression), correlation).isEmpty())) {
            return Optional.empty();
        }

        Expression condition = and(correlatedPredicates);
        Set<Symbol> conditionInputs = SymbolsExtractor.extractUnique(condition);

        PlanNode below = base;
        if (!uncorrelatedPredicates.isEmpty()) {
            below = new FilterNode(
                    idAllocator.getNextId(),
                    below,
                    and(uncorrelatedPredicates));
        }
        below = reapplyPassingConditionInputs(idAllocator, below, projectionsBelowLimit, conditionInputs);

        // The per-outer-row grouping of the limit is given by the base columns the join condition
        // uses; the condition then gates whole groups, so limiting before it is sound — but only
        // when each such column is group-constant (equality-correlated).
        ImmutableList.Builder<Symbol> partitionBuilder = ImmutableList.builder();
        for (Symbol symbol : below.getOutputSymbols()) {
            if (conditionInputs.contains(symbol)) {
                partitionBuilder.add(symbol);
            }
        }
        List<Symbol> partition = partitionBuilder.build();
        if (!groupConstantSymbols(condition, ImmutableSet.copyOf(below.getOutputSymbols()), typeCoercion).containsAll(partition)) {
            return Optional.empty();
        }

        PlanNode limited;
        long count = limit instanceof LimitNode limitNode ? limitNode.getCount() : ((TopNNode) limit).getCount();
        if (partition.isEmpty()) {
            // Nothing to partition by — the condition references only input columns and gates
            // whole (global) groups; keep the original bounding node below the join.
            limited = limit instanceof LimitNode
                    ? new LimitNode(idAllocator.getNextId(), below, count, false)
                    : new TopNNode(idAllocator.getNextId(), below, count, ((TopNNode) limit).getOrderingScheme(), TopNNode.Step.SINGLE);
        }
        else {
            Optional<OrderingScheme> ordering = limit instanceof TopNNode topN
                    ? stripConstantOrdering(topN.getOrderingScheme(), ImmutableSet.copyOf(partition), correlation)
                    : Optional.empty();
            if (ordering.isPresent()) {
                limited = new TopNRankingNode(
                        idAllocator.getNextId(),
                        below,
                        new DataOrganizationSpecification(partition, ordering),
                        TopNRankingNode.RankingType.ROW_NUMBER,
                        symbolAllocator.newSymbol("ranking", BIGINT),
                        toIntExact(count),
                        false);
            }
            else {
                limited = new RowNumberNode(
                        idAllocator.getNextId(),
                        below,
                        partition,
                        false,
                        symbolAllocator.newSymbol("row_number", BIGINT),
                        Optional.of(toIntExact(count)));
            }
        }

        PlanNode rightSide = reapplyPassingConditionInputs(idAllocator, limited, projectionsAboveLimit, conditionInputs);
        return Optional.of(new JoinNode(
                idAllocator.getNextId(),
                joinType,
                input,
                rightSide,
                ImmutableList.of(),
                input.getOutputSymbols(),
                rightSide.getOutputSymbols(),
                false,
                condition.equals(TRUE) ? Optional.empty() : Optional.of(condition),
                Optional.empty(),
                Optional.empty(),
                ImmutableMap.of(),
                Optional.empty()));
    }

    /// Drops the partition (group-constant) and correlation symbols from a TopN ordering — they
    /// are constant within each group, so they do not order anything there. Empty result means the
    /// per-group order is irrelevant and a plain row cap suffices.
    private static Optional<OrderingScheme> stripConstantOrdering(OrderingScheme orderingScheme, Set<Symbol> partition, Set<Symbol> correlation)
    {
        ImmutableList.Builder<Symbol> orderBy = ImmutableList.builder();
        ImmutableMap.Builder<Symbol, SortOrder> orderings = ImmutableMap.builder();
        for (Symbol symbol : orderingScheme.orderBy()) {
            if (!partition.contains(symbol) && !correlation.contains(symbol)) {
                orderBy.add(symbol);
                orderings.put(symbol, orderingScheme.ordering(symbol));
            }
        }
        List<Symbol> symbols = orderBy.build();
        if (symbols.isEmpty()) {
            return Optional.empty();
        }
        return Optional.of(new OrderingScheme(symbols, orderings.buildOrThrow()));
    }

    /// Decorrelates a LEFT dependent join to a plain LEFT join when the subquery is
    /// `π*(σ*(B))` with `B` correlation-free — i.e. the correlation appears only in
    /// liftable filter predicates (which become the join's ON condition) and projections. NULL
    /// correlation and duplicate outer rows are handled by the join itself (an unsatisfiable
    /// predicate null-extends; each outer row is matched independently), so no
    /// `IS NOT DISTINCT FROM` join-back or distinct set is needed.
    ///
    /// Correlation-free projections stay below the join on the null-supplying side, where the join
    /// null-extends them naturally (so constants and input-independent expressions come out NULL
    /// for unmatched rows, matching the empty subquery). A correlated suffix of the projection
    /// chain is lifted above the join, where the input columns are in scope — sound only when every
    /// lifted assignment derives from a subquery-side symbol (which the join null-extends);
    /// otherwise the magic-set handles it. Returns empty when the correlation reaches below the
    /// filters (e.g. into an aggregation).
    public Optional<PlanNode> tryPlainLeftJoin(PlanNode input, PlanNode subquery, Set<Symbol> correlation)
    {
        List<ProjectNode> projections = new ArrayList<>();
        List<Expression> predicates = new ArrayList<>();
        PlanNode node = lookup.resolve(subquery);
        while (true) {
            // Collect leading projections (above all filters) and the filter predicates beneath them.
            if (node instanceof ProjectNode project && predicates.isEmpty()) {
                projections.add(project);
                node = lookup.resolve(project.getSource());
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

        // Split the chain (outermost first in `projections`) at the outermost correlated
        // projection: everything at or below it must respect nesting, so the correlation-free
        // SUFFIX of the chain (the innermost part) goes below the join and the rest is lifted.
        int firstLifted = 0;
        for (int i = 0; i < projections.size(); i++) {
            boolean correlated = projections.get(i).getAssignments().expressions().stream()
                    .anyMatch(expression -> !Sets.intersection(SymbolsExtractor.extractUnique(expression), correlation).isEmpty());
            if (correlated) {
                firstLifted = i + 1;
            }
        }
        List<ProjectNode> lifted = projections.subList(0, firstLifted);
        List<ProjectNode> kept = projections.subList(firstLifted, projections.size());

        Expression condition = and(predicates);
        Set<Symbol> conditionInputs = SymbolsExtractor.extractUnique(condition);
        PlanNode rightSide = reapplyPassingConditionInputs(idAllocator, base, kept, conditionInputs);

        // A projection lifted above the LEFT join must yield NULL for null-extended (unmatched)
        // outer rows, matching the subquery producing no row. Referencing a null-extended symbol
        // is not enough — the expression must EVALUATE to NULL when those symbols are NULL
        // (`coalesce(t.x, o.id)` or `t.x IS NULL` reference the subquery side yet stay non-null).
        // Defer non-strict expressions to the magic-set, which keeps the projection below the
        // null-extending join-back. (Innermost projection first, so a projection's own outputs —
        // themselves null-on-null by induction — become null-extended inputs for the projection
        // above it.)
        Set<Symbol> subquerySideDerived = Sets.newHashSet(rightSide.getOutputSymbols());
        for (int i = lifted.size() - 1; i >= 0; i--) {
            for (Expression expression : lifted.get(i).getAssignments().expressions()) {
                if (!nullOnNull(expression, subquerySideDerived)) {
                    return Optional.empty();
                }
            }
            subquerySideDerived.addAll(lifted.get(i).getOutputSymbols());
        }

        PlanNode result = new JoinNode(
                idAllocator.getNextId(),
                JoinType.LEFT,
                input,
                rightSide,
                ImmutableList.of(),
                input.getOutputSymbols(),
                rightSide.getOutputSymbols(),
                false,
                condition.equals(TRUE) ? Optional.empty() : Optional.of(condition),
                Optional.empty(),
                Optional.empty(),
                ImmutableMap.of(),
                Optional.empty());

        // Re-apply the lifted projections (innermost first), passing the input columns through.
        for (int i = lifted.size() - 1; i >= 0; i--) {
            Assignments assignments = lifted.get(i).getAssignments();
            Assignments.Builder augmented = Assignments.builder().putAll(assignments);
            for (Symbol symbol : input.getOutputSymbols()) {
                if (!assignments.outputs().contains(symbol)) {
                    augmented.putIdentity(symbol);
                }
            }
            result = new ProjectNode(idAllocator.getNextId(), result, augmented.build());
        }
        return Optional.of(result);
    }

    /// A grouped aggregation over liftable filters, below a LEFT join — the form legacy reaches
    /// for a filter-only-correlated grouped subquery body (e.g. a scalar
    /// `(SELECT count(DISTINCT x) … GROUP BY y)`): the whole chain stays on the
    /// null-supplying side, with the aggregation's grouping keys — and an inner dedup's, when
    /// `count(DISTINCT …)` planned one — extended by the equality-pinned condition columns.
    /// Per outer row the join then selects exactly the groups whose pinned columns match, which is
    /// the original filter-then-group semantics; group-constancy is what makes the global grouping
    /// split coincide with the per-outer-row one. Everything below the join null-extends naturally
    /// for unmatched rows. Returns empty when the shape doesn't match or a condition column is not
    /// group-constant.
    public Optional<PlanNode> tryGroupedAggregationLeftJoin(PlanNode input, PlanNode body, Set<Symbol> correlation)
    {
        List<ProjectNode> projectionsAboveAggregation = new ArrayList<>();
        List<ProjectNode> projectionsBelowAggregation = new ArrayList<>();
        AggregationNode groupedAggregation = null;
        AggregationNode distinctOperator = null;
        List<Expression> correlatedPredicates = new ArrayList<>();
        List<Expression> uncorrelatedPredicates = new ArrayList<>();
        PlanNode node = lookup.resolve(body);
        while (true) {
            if (node instanceof ProjectNode project
                    && correlatedPredicates.isEmpty() && uncorrelatedPredicates.isEmpty()
                    && project.getAssignments().expressions().stream()
                    .allMatch(expression -> Sets.intersection(SymbolsExtractor.extractUnique(expression), correlation).isEmpty())) {
                (groupedAggregation == null ? projectionsAboveAggregation : projectionsBelowAggregation).add(project);
                node = lookup.resolve(project.getSource());
            }
            else if (node instanceof AggregationNode aggregation
                    && groupedAggregation == null
                    && distinctOperator == null
                    && correlatedPredicates.isEmpty() && uncorrelatedPredicates.isEmpty()
                    && !aggregation.getAggregations().isEmpty()
                    && aggregation.getStep() == AggregationNode.Step.SINGLE
                    && aggregation.getGroupingSetCount() == 1
                    && aggregation.hasNonEmptyGroupingSet()
                    && Sets.intersection(SymbolsExtractor.extractUniqueNonRecursive(aggregation), correlation).isEmpty()) {
                groupedAggregation = aggregation;
                node = lookup.resolve(aggregation.getSource());
            }
            else if (node instanceof AggregationNode distinct
                    && groupedAggregation != null
                    && distinctOperator == null
                    && correlatedPredicates.isEmpty() && uncorrelatedPredicates.isEmpty()
                    && isDistinctOperator(distinct)) {
                distinctOperator = distinct;
                node = lookup.resolve(distinct.getSource());
            }
            else if (node instanceof FilterNode filter && groupedAggregation != null) {
                for (Expression conjunct : extractConjuncts(filter.getPredicate())) {
                    boolean correlated = !Sets.intersection(SymbolsExtractor.extractUnique(conjunct), correlation).isEmpty();
                    (correlated ? correlatedPredicates : uncorrelatedPredicates).add(conjunct);
                }
                node = lookup.resolve(filter.getSource());
            }
            else {
                break;
            }
        }
        if (groupedAggregation == null || correlatedPredicates.isEmpty()) {
            return Optional.empty();
        }
        PlanNode base = node;
        if (!Sets.intersection(SymbolsExtractor.extractUnique(base, lookup), correlation).isEmpty()) {
            return Optional.empty();
        }

        Expression condition = and(correlatedPredicates);
        Set<Symbol> conditionInputs = SymbolsExtractor.extractUnique(condition);

        PlanNode build = base;
        if (!uncorrelatedPredicates.isEmpty()) {
            build = new FilterNode(
                    idAllocator.getNextId(),
                    build,
                    and(uncorrelatedPredicates));
        }
        ImmutableList.Builder<Symbol> pinnedBuilder = ImmutableList.builder();
        for (Symbol symbol : build.getOutputSymbols()) {
            if (conditionInputs.contains(symbol)) {
                pinnedBuilder.add(symbol);
            }
        }
        List<Symbol> pinned = pinnedBuilder.build();
        if (pinned.isEmpty()
                || !groupConstantSymbols(condition, ImmutableSet.copyOf(build.getOutputSymbols()), typeCoercion).containsAll(pinned)) {
            // Not group-constant (e.g. an inequality-correlated predicate). When the correlated
            // predicate references only the aggregation's grouping keys, it commutes above the
            // aggregation — filtering before vs. after grouping coincides for columns constant
            // within a group — so the aggregation runs correlation-free below a plain LEFT join
            // with the predicate (equality OR inequality) lifted into the ON. Clone-free and
            // uid-free: the input rides the join's probe side (duplicate outer rows stay distinct),
            // and an outer row with no matching group null-extends the aggregates to NULL — the
            // correct value for a grouped aggregation over an empty per-outer-row source (which
            // yields no row, unlike a global aggregate). Mirrors legacy's PlanNodeDecorrelator
            // lifting the predicate above the group. A count(DISTINCT) inner dedup is fine — its
            // keys include the grouping keys, so the predicate commutes above it too; a
            // below-aggregation projection declines (it could reintroduce non-key column
            // dependence). Crucially this is also what keeps a chain of such subqueries linear:
            // staying clone-free avoids the magic-set's input clone, whose duplication compounds
            // exponentially down a chain of correlated joins.
            if (!projectionsBelowAggregation.isEmpty()
                    || !groupedAggregation.getGroupingKeys().containsAll(pinned)) {
                return Optional.empty();
            }
            PlanNode belowAggregation = build;
            if (distinctOperator != null) {
                belowAggregation = new AggregationNode(
                        idAllocator.getNextId(),
                        belowAggregation,
                        ImmutableMap.of(),
                        singleGroupingSet(distinctOperator.getGroupingKeys()),
                        ImmutableList.of(),
                        AggregationNode.Step.SINGLE,
                        Optional.empty(),
                        Optional.empty());
            }
            PlanNode aggregatedBelow = new AggregationNode(
                    idAllocator.getNextId(),
                    belowAggregation,
                    groupedAggregation.getAggregations(),
                    singleGroupingSet(groupedAggregation.getGroupingKeys()),
                    ImmutableList.of(),
                    AggregationNode.Step.SINGLE,
                    Optional.empty(),
                    Optional.empty());
            aggregatedBelow = reapplyPassingConditionInputs(idAllocator, aggregatedBelow, projectionsAboveAggregation, conditionInputs);
            return Optional.of(new JoinNode(
                    idAllocator.getNextId(),
                    JoinType.LEFT,
                    input,
                    aggregatedBelow,
                    ImmutableList.of(),
                    input.getOutputSymbols(),
                    aggregatedBelow.getOutputSymbols(),
                    false,
                    Optional.of(condition),
                    Optional.empty(),
                    Optional.empty(),
                    ImmutableMap.of(),
                    Optional.empty()));
        }

        if (distinctOperator != null) {
            ImmutableList.Builder<Symbol> dedupKeys = ImmutableList.<Symbol>builder()
                    .addAll(distinctOperator.getGroupingKeys());
            for (Symbol symbol : pinned) {
                if (!distinctOperator.getGroupingKeys().contains(symbol)) {
                    dedupKeys.add(symbol);
                }
            }
            build = new AggregationNode(
                    idAllocator.getNextId(),
                    build,
                    ImmutableMap.of(),
                    singleGroupingSet(dedupKeys.build()),
                    ImmutableList.of(),
                    AggregationNode.Step.SINGLE,
                    Optional.empty(),
                    Optional.empty());
        }
        build = reapplyPassingConditionInputs(idAllocator, build, projectionsBelowAggregation, conditionInputs);
        ImmutableList.Builder<Symbol> aggregationKeys = ImmutableList.<Symbol>builder()
                .addAll(groupedAggregation.getGroupingKeys());
        for (Symbol symbol : pinned) {
            if (!groupedAggregation.getGroupingKeys().contains(symbol)) {
                aggregationKeys.add(symbol);
            }
        }
        build = new AggregationNode(
                idAllocator.getNextId(),
                build,
                groupedAggregation.getAggregations(),
                singleGroupingSet(aggregationKeys.build()),
                ImmutableList.of(),
                AggregationNode.Step.SINGLE,
                Optional.empty(),
                Optional.empty());
        build = reapplyPassingConditionInputs(idAllocator, build, projectionsAboveAggregation, conditionInputs);

        return Optional.of(new JoinNode(
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
                Optional.empty()));
    }

    /// True when the expression is guaranteed to evaluate to NULL whenever every symbol in
    /// `nullExtended` it references is NULL — the condition under which it may be lifted
    /// above a null-extending LEFT join. Conservative: bare references, casts, non-IDENTICAL
    /// comparisons, and calls to functions declared to return NULL on NULL input qualify; anything
    /// else (e.g. `coalesce`, `IS NULL`, `CASE`, logical operators) does not.
    private static boolean nullOnNull(Expression expression, Set<Symbol> nullExtended)
    {
        // Comparisons are calls to operator functions; decode so a non-IDENTICAL comparison is
        // null-on-null when either operand is, while IDENTICAL (which is total on NULL) is not.
        if (matchComparison(expression) instanceof Comparison comparison) {
            return !(comparison instanceof Comparison.Identical)
                    && (nullOnNull(comparison.left(), nullExtended) || nullOnNull(comparison.right(), nullExtended));
        }
        return switch (expression) {
            case Reference reference -> nullExtended.contains(Symbol.from(reference));
            case Cast cast -> nullOnNull(cast.expression(), nullExtended);
            case Call call -> {
                List<Boolean> argumentNullable = call.function().functionNullability().getArgumentNullable();
                for (int i = 0; i < call.arguments().size(); i++) {
                    if (!argumentNullable.get(i) && nullOnNull(call.arguments().get(i), nullExtended)) {
                        yield true;
                    }
                }
                yield false;
            }
            default -> false;
        };
    }
}
