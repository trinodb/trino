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
package io.trino.sql.planner.optimizations;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import io.trino.Session;
import io.trino.spi.type.Type;
import io.trino.sql.PlannerContext;
import io.trino.sql.ir.Constant;
import io.trino.sql.ir.Expression;
import io.trino.sql.ir.Reference;
import io.trino.sql.ir.Row;
import io.trino.sql.ir.optimizer.IrExpressionOptimizer;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.SymbolAllocator;
import io.trino.sql.planner.SymbolsExtractor;
import io.trino.sql.planner.plan.AggregationNode;
import io.trino.sql.planner.plan.ApplyNode;
import io.trino.sql.planner.plan.AssignUniqueId;
import io.trino.sql.planner.plan.CorrelatedJoinNode;
import io.trino.sql.planner.plan.DistinctLimitNode;
import io.trino.sql.planner.plan.EnforceSingleRowNode;
import io.trino.sql.planner.plan.ExceptNode;
import io.trino.sql.planner.plan.ExchangeNode;
import io.trino.sql.planner.plan.FilterNode;
import io.trino.sql.planner.plan.GroupIdNode;
import io.trino.sql.planner.plan.IndexJoinNode;
import io.trino.sql.planner.plan.IntersectNode;
import io.trino.sql.planner.plan.JoinNode;
import io.trino.sql.planner.plan.LimitNode;
import io.trino.sql.planner.plan.MarkDistinctNode;
import io.trino.sql.planner.plan.OffsetNode;
import io.trino.sql.planner.plan.OutputNode;
import io.trino.sql.planner.plan.PlanNode;
import io.trino.sql.planner.plan.PlanVisitor;
import io.trino.sql.planner.plan.ProjectNode;
import io.trino.sql.planner.plan.RowNumberNode;
import io.trino.sql.planner.plan.SampleNode;
import io.trino.sql.planner.plan.SemiJoinNode;
import io.trino.sql.planner.plan.SetOperationNode;
import io.trino.sql.planner.plan.SortNode;
import io.trino.sql.planner.plan.SpatialJoinNode;
import io.trino.sql.planner.plan.TableScanNode;
import io.trino.sql.planner.plan.TopNNode;
import io.trino.sql.planner.plan.TopNRankingNode;
import io.trino.sql.planner.plan.UnionNode;
import io.trino.sql.planner.plan.UnnestNode;
import io.trino.sql.planner.plan.ValuesNode;
import io.trino.sql.planner.plan.WindowNode;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.IntStream;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.trino.SystemSessionProperties.getCharVarcharCoercion;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.sql.ir.IrExpressions.mayBeNull;
import static io.trino.sql.ir.IrUtils.extractConjuncts;
import static io.trino.sql.ir.optimizer.IrExpressionOptimizer.newOptimizer;
import static io.trino.sql.planner.DeterminismEvaluator.isDeterministic;
import static io.trino.sql.planner.plan.JoinType.INNER;
import static java.util.Objects.requireNonNull;
import static java.util.function.Predicate.not;

/// Derives the set of symbols that are guaranteed to be non-null in the output of a plan node.
///
/// The seed facts come from table scans over columns declared `NOT NULL` by the connector
/// (`ColumnMetadata.isNullable() == false`, e.g. required Iceberg fields or database-enforced
/// constraints in JDBC connectors) and from null-rejecting filter predicates. Facts propagate
/// through operators that preserve values and are destroyed by operators that introduce nulls:
/// the null-extended side of an outer, correlated, or spatial join, grouping columns absent from
/// some grouping set of a `GROUP ID` node, and scalar subquery coercion (`EnforceSingleRowNode`,
/// which produces a single all-null row over an empty source).
///
/// The default for unhandled nodes is the empty set, so the derivation is always sound. The
/// result contains only symbols that are part of the node's output.
///
/// Table-scan facts trust the column nullability the connector reports for the scanned table
/// handle. That is only valid while null-introducing operations have not been pushed into the
/// connector: a handle produced by outer-join pushdown may keep reporting `NOT NULL` for columns
/// of the null-extended side. Callers must run before connector join pushdown
/// (`PushJoinIntoTableScan`), as the current callers do.
public final class NonNullDerivation
{
    private final PlannerContext plannerContext;
    private final IrExpressionOptimizer evaluator;

    public NonNullDerivation(PlannerContext plannerContext)
    {
        this.plannerContext = requireNonNull(plannerContext, "plannerContext is null");
        this.evaluator = newOptimizer(plannerContext);
    }

    /// Derives the non-null output symbols of a single node. Child groups are resolved through the
    /// given [NonNullProvider], which caches per-group results in the [io.trino.sql.planner.iterative.Memo]
    /// (mirroring how stats and cost are cached).
    public Set<Symbol> calculate(PlanNode node, NonNullProvider sources, Session session)
    {
        Set<Symbol> nonNull = node.accept(new Visitor(sources, session), null);
        // The result is restricted to the node's output: a symbol not in the output cannot be
        // referenced upstream, and keeping this invariant makes the per-node cached value stable.
        return Sets.intersection(nonNull, ImmutableSet.copyOf(node.getOutputSymbols())).immutableCopy();
    }

    /// Convenience entry point for callers without a [io.trino.sql.planner.iterative.Memo] (e.g. tests).
    public static Set<Symbol> deriveNonNullSymbols(PlannerContext plannerContext, Session session, PlanNode node)
    {
        return new CachingNonNullProvider(new NonNullDerivation(plannerContext), Optional.empty(), session).getNonNullSymbols(node);
    }

    private final class Visitor
            extends PlanVisitor<Set<Symbol>, Void>
    {
        // Resolves child groups (and caches their results); with the iterative optimizer children
        // are GroupReference nodes that the provider resolves through the Memo.
        private final NonNullProvider sources;
        private final Session session;

        private Visitor(NonNullProvider sources, Session session)
        {
            this.sources = requireNonNull(sources, "sources is null");
            this.session = requireNonNull(session, "session is null");
        }

        private Set<Symbol> derive(PlanNode node)
        {
            return sources.getNonNullSymbols(node);
        }

        @Override
        protected Set<Symbol> visitPlan(PlanNode node, Void context)
        {
            return ImmutableSet.of();
        }

        @Override
        public Set<Symbol> visitTableScan(TableScanNode node, Void context)
        {
            return node.getAssignments().entrySet().stream()
                    .filter(entry -> !plannerContext.getMetadata().getColumnMetadata(session, node.getTable(), entry.getValue()).isNullable())
                    .map(Map.Entry::getKey)
                    .collect(toImmutableSet());
        }

        @Override
        public Set<Symbol> visitFilter(FilterNode node, Void context)
        {
            Set<Symbol> nonNull = new HashSet<>(derive(node.getSource()));
            nonNull.addAll(nullRejectedSymbols(node.getPredicate(), nonNull));
            return ImmutableSet.copyOf(nonNull);
        }

        /// Symbols for which some deterministic conjunct of the predicate rejects nulls, excluding
        /// those already known to be non-null.
        private Set<Symbol> nullRejectedSymbols(Expression predicate, Set<Symbol> knownNonNull)
        {
            Set<Symbol> result = new HashSet<>();
            for (Expression conjunct : extractConjuncts(predicate)) {
                if (!isDeterministic(conjunct)) {
                    continue;
                }
                SymbolsExtractor.extractUnique(conjunct).stream()
                        .filter(not(knownNonNull::contains))
                        .filter(not(result::contains))
                        .filter(symbol -> rejectsNull(conjunct, symbol))
                        .forEach(result::add);
            }
            return result;
        }

        /// A conjunct that evaluates to FALSE or NULL whenever the symbol is null filters out
        /// all rows in which the symbol is null, so the symbol is non-null downstream.
        private boolean rejectsNull(Expression conjunct, Symbol symbol)
        {
            SymbolAllocator symbolAllocator = new SymbolAllocator(SymbolsExtractor.extractUnique(conjunct));
            Expression response = evaluator.process(conjunct, session, symbolAllocator, Map.of(symbol, new Constant(symbol.type(), null)))
                    .orElse(conjunct);
            return response instanceof Constant(Type type, Object value) &&
                    type.equals(BOOLEAN) &&
                    (value == null || Boolean.FALSE.equals(value));
        }

        @Override
        public Set<Symbol> visitProject(ProjectNode node, Void context)
        {
            Set<Symbol> sourceNonNull = derive(node.getSource());
            Predicate<Reference> referenceMayBeNull = reference -> !sourceNonNull.contains(Symbol.from(reference));
            return node.getAssignments().entrySet().stream()
                    .filter(entry -> !mayBeNull(plannerContext, getCharVarcharCoercion(session), entry.getValue(), referenceMayBeNull))
                    .map(Map.Entry::getKey)
                    .collect(toImmutableSet());
        }

        @Override
        public Set<Symbol> visitJoin(JoinNode node, Void context)
        {
            return switch (node.getType()) {
                case INNER -> {
                    Set<Symbol> result = new HashSet<>(derive(node.getLeft()));
                    result.addAll(derive(node.getRight()));
                    // equi-join criteria reject nulls on both sides
                    for (JoinNode.EquiJoinClause clause : node.getCriteria()) {
                        result.add(clause.getLeft());
                        result.add(clause.getRight());
                    }
                    // an inner join filter rejects rows like a FilterNode predicate
                    node.getFilter().ifPresent(filter -> result.addAll(nullRejectedSymbols(filter, result)));
                    yield ImmutableSet.copyOf(result);
                }
                case LEFT -> derive(node.getLeft());
                case RIGHT -> derive(node.getRight());
                case FULL -> ImmutableSet.of();
            };
        }

        @Override
        public Set<Symbol> visitSemiJoin(SemiJoinNode node, Void context)
        {
            // the match symbol is excluded: NULL IN (...) semantics can produce a null result
            return derive(node.getSource());
        }

        @Override
        public Set<Symbol> visitCorrelatedJoin(CorrelatedJoinNode node, Void context)
        {
            return switch (node.getType()) {
                case INNER -> {
                    Set<Symbol> result = new HashSet<>(derive(node.getInput()));
                    result.addAll(derive(node.getSubquery()));
                    result.addAll(nullRejectedSymbols(node.getFilter(), result));
                    yield ImmutableSet.copyOf(result);
                }
                // the subquery side is null-extended for input rows with an empty subquery result
                case LEFT -> derive(node.getInput());
                // not produced by the planner; conservatively assume both sides may be null-extended
                case RIGHT, FULL -> ImmutableSet.of();
            };
        }

        @Override
        public Set<Symbol> visitApply(ApplyNode node, Void context)
        {
            // every input row is preserved, extended with the subquery expression results;
            // EXISTS is never null, while IN and quantified comparisons follow
            // NULL IN (...) semantics and can produce a null result
            ImmutableSet.Builder<Symbol> result = ImmutableSet.builder();
            result.addAll(derive(node.getInput()));
            node.getSubqueryAssignments().entrySet().stream()
                    .filter(entry -> entry.getValue() instanceof ApplyNode.Exists)
                    .forEach(entry -> result.add(entry.getKey()));
            return result.build();
        }

        @Override
        public Set<Symbol> visitAggregation(AggregationNode node, Void context)
        {
            Set<Symbol> sourceNonNull = derive(node.getSource());
            ImmutableSet.Builder<Symbol> result = ImmutableSet.builder();
            node.getGroupingKeys().stream()
                    .filter(sourceNonNull::contains)
                    .forEach(result::add);
            if (!node.getStep().isOutputPartial()) {
                // an aggregation whose function declares a non-null result (e.g. count, count_if,
                // approx_distinct) never produces null, regardless of grouping or filtering
                node.getAggregations().entrySet().stream()
                        .filter(entry -> !entry.getValue().getResolvedFunction().functionNullability().isReturnNullable())
                        .forEach(entry -> result.add(entry.getKey()));
            }
            return result.build();
        }

        @Override
        public Set<Symbol> visitGroupId(GroupIdNode node, Void context)
        {
            Set<Symbol> sourceNonNull = derive(node.getSource());
            ImmutableSet.Builder<Symbol> result = ImmutableSet.builder();
            result.add(node.getGroupIdSymbol());
            node.getAggregationArguments().stream()
                    .filter(sourceNonNull::contains)
                    .forEach(result::add);
            // a grouping column is nulled out in every grouping set it does not participate in
            node.getGroupingColumns().entrySet().stream()
                    .filter(entry -> sourceNonNull.contains(entry.getValue()))
                    .filter(entry -> node.getGroupingSets().stream().allMatch(set -> set.contains(entry.getKey())))
                    .forEach(entry -> result.add(entry.getKey()));
            return result.build();
        }

        @Override
        public Set<Symbol> visitWindow(WindowNode node, Void context)
        {
            ImmutableSet.Builder<Symbol> result = ImmutableSet.builder();
            result.addAll(derive(node.getSource()));
            // a window function output is non-null when the function declares a non-null result
            // (e.g. the ranking functions and count), regardless of frame or input nulls
            node.getWindowFunctions().entrySet().stream()
                    .filter(entry -> !entry.getValue().getResolvedFunction().functionNullability().isReturnNullable())
                    .forEach(entry -> result.add(entry.getKey()));
            return result.build();
        }

        @Override
        public Set<Symbol> visitSpatialJoin(SpatialJoinNode node, Void context)
        {
            return switch (node.getType()) {
                case INNER -> {
                    Set<Symbol> result = new HashSet<>(derive(node.getLeft()));
                    result.addAll(derive(node.getRight()));
                    result.addAll(nullRejectedSymbols(node.getFilter(), result));
                    yield ImmutableSet.copyOf(result);
                }
                case LEFT -> derive(node.getLeft());
            };
        }

        @Override
        public Set<Symbol> visitIndexJoin(IndexJoinNode node, Void context)
        {
            return switch (node.getType()) {
                case INNER -> {
                    Set<Symbol> result = new HashSet<>(derive(node.getProbeSource()));
                    result.addAll(derive(node.getIndexSource()));
                    // equi-join criteria reject nulls on both sides
                    for (IndexJoinNode.EquiJoinClause clause : node.getCriteria()) {
                        result.add(clause.getProbe());
                        result.add(clause.getIndex());
                    }
                    yield ImmutableSet.copyOf(result);
                }
                case SOURCE_OUTER -> derive(node.getProbeSource());
            };
        }

        @Override
        public Set<Symbol> visitExchange(ExchangeNode node, Void context)
        {
            return derivePerSourceIntersection(node.getOutputSymbols(), node.getSources(), node.getInputs());
        }

        @Override
        public Set<Symbol> visitUnion(UnionNode node, Void context)
        {
            return visitSetOperation(node);
        }

        @Override
        public Set<Symbol> visitIntersect(IntersectNode node, Void context)
        {
            return visitSetOperation(node);
        }

        @Override
        public Set<Symbol> visitExcept(ExceptNode node, Void context)
        {
            return visitSetOperation(node);
        }

        private Set<Symbol> visitSetOperation(SetOperationNode node)
        {
            List<List<Symbol>> inputs = IntStream.range(0, node.getSources().size())
                    .mapToObj(node::sourceOutputLayout)
                    .collect(toImmutableList());
            return derivePerSourceIntersection(node.getOutputSymbols(), node.getSources(), inputs);
        }

        /// An output symbol is non-null if the input feeding it is non-null in every source.
        private Set<Symbol> derivePerSourceIntersection(List<Symbol> outputs, List<PlanNode> sources, List<List<Symbol>> inputs)
        {
            List<Set<Symbol>> sourceNonNull = sources.stream()
                    .map(this::derive)
                    .collect(toImmutableList());
            ImmutableSet.Builder<Symbol> result = ImmutableSet.builder();
            for (int output = 0; output < outputs.size(); output++) {
                boolean nonNull = true;
                for (int source = 0; source < sources.size(); source++) {
                    if (!sourceNonNull.get(source).contains(inputs.get(source).get(output))) {
                        nonNull = false;
                        break;
                    }
                }
                if (nonNull) {
                    result.add(outputs.get(output));
                }
            }
            return result.build();
        }

        @Override
        public Set<Symbol> visitValues(ValuesNode node, Void context)
        {
            if (node.getRows().isEmpty() || node.getRows().get().stream().anyMatch(row -> !(row instanceof Row))) {
                return ImmutableSet.of();
            }
            ImmutableSet.Builder<Symbol> result = ImmutableSet.builder();
            for (int i = 0; i < node.getOutputSymbols().size(); i++) {
                int field = i;
                boolean nonNull = node.getRows().get().stream()
                        .noneMatch(row -> mayBeNull(plannerContext, getCharVarcharCoercion(session), ((Row) row).items().get(field)));
                if (nonNull) {
                    result.add(node.getOutputSymbols().get(i));
                }
            }
            return result.build();
        }

        @Override
        public Set<Symbol> visitUnnest(UnnestNode node, Void context)
        {
            Set<Symbol> sourceNonNull = derive(node.getSource());
            ImmutableSet.Builder<Symbol> result = ImmutableSet.builder();
            node.getReplicateSymbols().stream()
                    .filter(sourceNonNull::contains)
                    .forEach(result::add);
            if (node.getJoinType() == INNER) {
                node.getOrdinalitySymbol().ifPresent(result::add);
            }
            return result.build();
        }

        @Override
        public Set<Symbol> visitMarkDistinct(MarkDistinctNode node, Void context)
        {
            return ImmutableSet.<Symbol>builder()
                    .addAll(derive(node.getSource()))
                    .add(node.getMarkerSymbol())
                    .build();
        }

        @Override
        public Set<Symbol> visitRowNumber(RowNumberNode node, Void context)
        {
            return ImmutableSet.<Symbol>builder()
                    .addAll(derive(node.getSource()))
                    .add(node.getRowNumberSymbol())
                    .build();
        }

        @Override
        public Set<Symbol> visitTopNRanking(TopNRankingNode node, Void context)
        {
            return ImmutableSet.<Symbol>builder()
                    .addAll(derive(node.getSource()))
                    .add(node.getRankingSymbol())
                    .build();
        }

        @Override
        public Set<Symbol> visitAssignUniqueId(AssignUniqueId node, Void context)
        {
            return ImmutableSet.<Symbol>builder()
                    .addAll(derive(node.getSource()))
                    .add(node.getIdColumn())
                    .build();
        }

        @Override
        public Set<Symbol> visitSort(SortNode node, Void context)
        {
            return derive(node.getSource());
        }

        @Override
        public Set<Symbol> visitTopN(TopNNode node, Void context)
        {
            return derive(node.getSource());
        }

        @Override
        public Set<Symbol> visitLimit(LimitNode node, Void context)
        {
            return derive(node.getSource());
        }

        @Override
        public Set<Symbol> visitOffset(OffsetNode node, Void context)
        {
            return derive(node.getSource());
        }

        @Override
        public Set<Symbol> visitDistinctLimit(DistinctLimitNode node, Void context)
        {
            return derive(node.getSource());
        }

        @Override
        public Set<Symbol> visitSample(SampleNode node, Void context)
        {
            return derive(node.getSource());
        }

        @Override
        public Set<Symbol> visitOutput(OutputNode node, Void context)
        {
            return derive(node.getSource());
        }

        @Override
        public Set<Symbol> visitEnforceSingleRow(EnforceSingleRowNode node, Void context)
        {
            // scalar subquery semantics: an empty source produces a single all-null row, so no
            // source fact survives; this must remain the empty set and is spelled out explicitly
            // to keep it from being "fixed" into a pass-through
            return ImmutableSet.of();
        }
    }
}
