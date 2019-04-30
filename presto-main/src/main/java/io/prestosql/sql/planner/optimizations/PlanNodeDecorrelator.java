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

package io.prestosql.sql.planner.optimizations;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;
import io.prestosql.metadata.Metadata;
import io.prestosql.spi.block.SortOrder;
import io.prestosql.sql.ExpressionUtils;
import io.prestosql.sql.planner.OrderingScheme;
import io.prestosql.sql.planner.Symbol;
import io.prestosql.sql.planner.SymbolAllocator;
import io.prestosql.sql.planner.SymbolsExtractor;
import io.prestosql.sql.planner.iterative.GroupReference;
import io.prestosql.sql.planner.iterative.Lookup;
import io.prestosql.sql.planner.plan.AggregationNode;
import io.prestosql.sql.planner.plan.Assignments;
import io.prestosql.sql.planner.plan.EnforceSingleRowNode;
import io.prestosql.sql.planner.plan.FilterNode;
import io.prestosql.sql.planner.plan.LimitNode;
import io.prestosql.sql.planner.plan.PlanNode;
import io.prestosql.sql.planner.plan.PlanNodeId;
import io.prestosql.sql.planner.plan.PlanVisitor;
import io.prestosql.sql.planner.plan.ProjectNode;
import io.prestosql.sql.planner.plan.RowNumberNode;
import io.prestosql.sql.planner.plan.TopNNode;
import io.prestosql.sql.planner.plan.TopNRowNumberNode;
import io.prestosql.sql.planner.plan.WindowNode.Specification;
import io.prestosql.sql.tree.ComparisonExpression;
import io.prestosql.sql.tree.Expression;
import io.prestosql.sql.tree.SymbolReference;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static io.prestosql.spi.type.BigintType.BIGINT;
import static io.prestosql.sql.planner.plan.AggregationNode.singleGroupingSet;
import static io.prestosql.sql.tree.ComparisonExpression.Operator.EQUAL;
import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;

public class PlanNodeDecorrelator
{
    private final Metadata metadata;
    private final SymbolAllocator symbolAllocator;
    private final Lookup lookup;

    public PlanNodeDecorrelator(Metadata metadata, SymbolAllocator symbolAllocator, Lookup lookup)
    {
        this.metadata = requireNonNull(metadata, "metadata is null");
        this.symbolAllocator = requireNonNull(symbolAllocator, "symbolAllocator is null");
        this.lookup = requireNonNull(lookup, "lookup is null");
    }

    public Optional<DecorrelatedNode> decorrelateFilters(PlanNode node, List<Symbol> correlation)
    {
        // TODO: when correlations list empty this should return immediately. However this isn't correct
        // right now, because for nested subqueries correlation list is empty while there might exists usages
        // of the outer most correlated symbols

        Optional<DecorrelationResult> decorrelationResultOptional = node.accept(new DecorrelatingVisitor(metadata, correlation), null);
        return decorrelationResultOptional.flatMap(decorrelationResult -> decorrelatedNode(
                decorrelationResult.correlatedPredicates,
                decorrelationResult.node,
                correlation));
    }

    private class DecorrelatingVisitor
            extends PlanVisitor<Optional<DecorrelationResult>, Void>
    {
        private final Metadata metadata;
        private final List<Symbol> correlation;

        DecorrelatingVisitor(Metadata metadata, List<Symbol> correlation)
        {
            this.metadata = metadata;
            this.correlation = requireNonNull(correlation, "correlation is null");
        }

        @Override
        protected Optional<DecorrelationResult> visitPlan(PlanNode node, Void context)
        {
            if (containsCorrelation(node, correlation)) {
                return Optional.empty();
            }
            return Optional.of(new DecorrelationResult(
                    node,
                    ImmutableSet.of(),
                    ImmutableList.of(),
                    ImmutableMultimap.of(),
                    false));
        }

        @Override
        public Optional<DecorrelationResult> visitGroupReference(GroupReference node, Void context)
        {
            return lookup.resolve(node).accept(this, null);
        }

        @Override
        public Optional<DecorrelationResult> visitFilter(FilterNode node, Void context)
        {
            Optional<DecorrelationResult> childDecorrelationResultOptional = Optional.of(new DecorrelationResult(
                    node.getSource(),
                    ImmutableSet.of(),
                    ImmutableList.of(),
                    ImmutableMultimap.of(),
                    false));

            // try to decorrelate filters down the tree
            if (containsCorrelation(node.getSource(), correlation)) {
                childDecorrelationResultOptional = node.getSource().accept(this, null);
            }

            if (!childDecorrelationResultOptional.isPresent()) {
                return Optional.empty();
            }

            Expression predicate = node.getPredicate();
            Map<Boolean, List<Expression>> predicates = ExpressionUtils.extractConjuncts(predicate).stream()
                    .collect(Collectors.partitioningBy(PlanNodeDecorrelator.DecorrelatingVisitor.this::isCorrelated));
            List<Expression> correlatedPredicates = ImmutableList.copyOf(predicates.get(true));
            List<Expression> uncorrelatedPredicates = ImmutableList.copyOf(predicates.get(false));

            DecorrelationResult childDecorrelationResult = childDecorrelationResultOptional.get();
            FilterNode newFilterNode = new FilterNode(
                    node.getId(),
                    childDecorrelationResult.node,
                    ExpressionUtils.combineConjuncts(metadata, uncorrelatedPredicates));

            Set<Symbol> symbolsToPropagate = Sets.difference(SymbolsExtractor.extractUnique(correlatedPredicates), ImmutableSet.copyOf(correlation));
            return Optional.of(new DecorrelationResult(
                    newFilterNode,
                    Sets.union(childDecorrelationResult.symbolsToPropagate, symbolsToPropagate),
                    ImmutableList.<Expression>builder()
                            .addAll(childDecorrelationResult.correlatedPredicates)
                            .addAll(correlatedPredicates)
                            .build(),
                    ImmutableMultimap.<Symbol, Symbol>builder()
                            .putAll(childDecorrelationResult.correlatedSymbolsMapping)
                            .putAll(extractCorrelatedSymbolsMapping(correlatedPredicates))
                            .build(),
                    childDecorrelationResult.atMostSingleRow));
        }

        @Override
        public Optional<DecorrelationResult> visitLimit(LimitNode node, Void context)
        {
            if (node.getCount() == 0 || node.isWithTies()) {
                return Optional.empty();
            }

            Optional<DecorrelationResult> childDecorrelationResultOptional = node.getSource().accept(this, null);
            if (!childDecorrelationResultOptional.isPresent()) {
                return Optional.empty();
            }

            DecorrelationResult childDecorrelationResult = childDecorrelationResultOptional.get();
            if (childDecorrelationResult.atMostSingleRow) {
                return childDecorrelationResultOptional;
            }

            if (node.getCount() == 1) {
                return rewriteLimitWithRowCountOne(childDecorrelationResult, node.getId());
            }
            return rewriteLimitWithRowCountGreaterThanOne(childDecorrelationResult, node);
        }

        // TODO Limit (1) could be decorrelated by the method rewriteLimitWithRowCountGreaterThanOne() as well.
        // The current decorrelation method for Limit (1) cannot deal with subqueries outputting other symbols
        // than constants.
        //
        // An example query that is currently not supported:
        // SELECT (
        //      SELECT a+b
        //      FROM (VALUES (1, 2), (1, 2)) inner_relation(a, b)
        //      WHERE a=x
        //      LIMIT 1)
        // FROM (VALUES (1)) outer_relation(x)
        //
        // Switching the decorrelation method would change the way that queries with EXISTS are executed,
        // and thus it needs benchmarking.
        private Optional<DecorrelationResult> rewriteLimitWithRowCountOne(DecorrelationResult childDecorrelationResult, PlanNodeId nodeId)
        {
            PlanNode decorrelatedChildNode = childDecorrelationResult.node;
            Set<Symbol> constantSymbols = childDecorrelationResult.getConstantSymbols();

            if (constantSymbols.isEmpty() || !constantSymbols.containsAll(decorrelatedChildNode.getOutputSymbols())) {
                return Optional.empty();
            }

            // rewrite Limit to aggregation on constant symbols
            AggregationNode aggregationNode = new AggregationNode(
                    nodeId,
                    decorrelatedChildNode,
                    ImmutableMap.of(),
                    singleGroupingSet(decorrelatedChildNode.getOutputSymbols()),
                    ImmutableList.of(),
                    AggregationNode.Step.SINGLE,
                    Optional.empty(),
                    Optional.empty());

            return Optional.of(new DecorrelationResult(
                    aggregationNode,
                    childDecorrelationResult.symbolsToPropagate,
                    childDecorrelationResult.correlatedPredicates,
                    childDecorrelationResult.correlatedSymbolsMapping,
                    true));
        }

        private Optional<DecorrelationResult> rewriteLimitWithRowCountGreaterThanOne(DecorrelationResult childDecorrelationResult, LimitNode node)
        {
            PlanNode decorrelatedChildNode = childDecorrelationResult.node;

            // no rewrite needed (no symbols to partition by)
            if (childDecorrelationResult.symbolsToPropagate.isEmpty()) {
                return Optional.of(new DecorrelationResult(
                        node.replaceChildren(ImmutableList.of(decorrelatedChildNode)),
                        childDecorrelationResult.symbolsToPropagate,
                        childDecorrelationResult.correlatedPredicates,
                        childDecorrelationResult.correlatedSymbolsMapping,
                        false));
            }

            Set<Symbol> constantSymbols = childDecorrelationResult.getConstantSymbols();
            if (!constantSymbols.containsAll(childDecorrelationResult.symbolsToPropagate)) {
                return Optional.empty();
            }

            // rewrite Limit to RowNumberNode partitioned by constant symbols
            RowNumberNode rowNumberNode = new RowNumberNode(
                    node.getId(),
                    decorrelatedChildNode,
                    ImmutableList.copyOf(childDecorrelationResult.symbolsToPropagate),
                    symbolAllocator.newSymbol("row_number", BIGINT),
                    Optional.of(toIntExact(node.getCount())),
                    Optional.empty());

            return Optional.of(new DecorrelationResult(
                    rowNumberNode,
                    childDecorrelationResult.symbolsToPropagate,
                    childDecorrelationResult.correlatedPredicates,
                    childDecorrelationResult.correlatedSymbolsMapping,
                    false));
        }

        @Override
        public Optional<DecorrelationResult> visitTopN(TopNNode node, Void context)
        {
            if (node.getCount() == 0) {
                return Optional.empty();
            }

            Optional<DecorrelationResult> childDecorrelationResultOptional = node.getSource().accept(this, null);
            if (!childDecorrelationResultOptional.isPresent()) {
                return Optional.empty();
            }

            DecorrelationResult childDecorrelationResult = childDecorrelationResultOptional.get();
            if (childDecorrelationResult.atMostSingleRow) {
                return childDecorrelationResultOptional;
            }

            PlanNode decorrelatedChildNode = childDecorrelationResult.node;
            Set<Symbol> constantSymbols = childDecorrelationResult.getConstantSymbols();
            Optional<OrderingScheme> decorrelatedOrderingScheme = decorrelateOrderingScheme(node.getOrderingScheme(), constantSymbols);

            // no partitioning needed (no symbols to partition by)
            if (childDecorrelationResult.symbolsToPropagate.isEmpty()) {
                return decorrelatedOrderingScheme
                        .map(orderingScheme -> Optional.of(new DecorrelationResult(
                                // ordering symbols are present - return decorrelated TopNNode
                                new TopNNode(node.getId(), decorrelatedChildNode, node.getCount(), orderingScheme, node.getStep()),
                                childDecorrelationResult.symbolsToPropagate,
                                childDecorrelationResult.correlatedPredicates,
                                childDecorrelationResult.correlatedSymbolsMapping,
                                node.getCount() == 1)))
                        .orElseGet(() -> Optional.of(new DecorrelationResult(
                                // no ordering symbols are left - convert to LimitNode
                                new LimitNode(node.getId(), decorrelatedChildNode, node.getCount(), false),
                                childDecorrelationResult.symbolsToPropagate,
                                childDecorrelationResult.correlatedPredicates,
                                childDecorrelationResult.correlatedSymbolsMapping,
                                node.getCount() == 1)));
            }

            if (!constantSymbols.containsAll(childDecorrelationResult.symbolsToPropagate)) {
                return Optional.empty();
            }

            return decorrelatedOrderingScheme
                    .map(orderingScheme -> {
                        // ordering symbols are present - rewrite TopN to TopNRowNumberNode partitioned by constant symbols
                        TopNRowNumberNode topNRowNumberNode = new TopNRowNumberNode(
                                node.getId(),
                                decorrelatedChildNode,
                                new Specification(
                                        ImmutableList.copyOf(childDecorrelationResult.symbolsToPropagate),
                                        Optional.of(orderingScheme)),
                                symbolAllocator.newSymbol("row_number", BIGINT),
                                toIntExact(node.getCount()),
                                false,
                                Optional.empty());

                        return Optional.of(new DecorrelationResult(
                                topNRowNumberNode,
                                childDecorrelationResult.symbolsToPropagate,
                                childDecorrelationResult.correlatedPredicates,
                                childDecorrelationResult.correlatedSymbolsMapping,
                                node.getCount() == 1));
                    })
                    .orElseGet(() -> {
                        // no ordering symbols are left - rewrite TopN to RowNumberNode partitioned by constant symbols
                        RowNumberNode rowNumberNode = new RowNumberNode(
                                node.getId(),
                                decorrelatedChildNode,
                                ImmutableList.copyOf(childDecorrelationResult.symbolsToPropagate),
                                symbolAllocator.newSymbol("row_number", BIGINT),
                                Optional.of(toIntExact(node.getCount())),
                                Optional.empty());

                        return Optional.of(new DecorrelationResult(
                                rowNumberNode,
                                childDecorrelationResult.symbolsToPropagate,
                                childDecorrelationResult.correlatedPredicates,
                                childDecorrelationResult.correlatedSymbolsMapping,
                                node.getCount() == 1));
                    });
        }

        private Optional<OrderingScheme> decorrelateOrderingScheme(OrderingScheme orderingScheme, Set<Symbol> constantSymbols)
        {
            // remove local and remote constant sort symbols from the OrderingScheme
            ImmutableList.Builder<Symbol> nonConstantOrderBy = ImmutableList.builder();
            ImmutableMap.Builder<Symbol, SortOrder> nonConstantOrderings = ImmutableMap.builder();
            for (Symbol symbol : orderingScheme.getOrderBy()) {
                if (!constantSymbols.contains(symbol) && !correlation.contains(symbol)) {
                    nonConstantOrderBy.add(symbol);
                    nonConstantOrderings.put(symbol, orderingScheme.getOrdering(symbol));
                }
            }
            if (nonConstantOrderBy.build().isEmpty()) {
                return Optional.empty();
            }
            return Optional.of(new OrderingScheme(nonConstantOrderBy.build(), nonConstantOrderings.build()));
        }

        @Override
        public Optional<DecorrelationResult> visitEnforceSingleRow(EnforceSingleRowNode node, Void context)
        {
            Optional<DecorrelationResult> childDecorrelationResultOptional = node.getSource().accept(this, null);
            return childDecorrelationResultOptional.filter(result -> result.atMostSingleRow);
        }

        @Override
        public Optional<DecorrelationResult> visitAggregation(AggregationNode node, Void context)
        {
            if (node.hasEmptyGroupingSet()) {
                return Optional.empty();
            }

            if (node.getGroupingSetCount() != 1) {
                return Optional.empty();
            }

            Optional<DecorrelationResult> childDecorrelationResultOptional = node.getSource().accept(this, null);
            if (!childDecorrelationResultOptional.isPresent()) {
                return Optional.empty();
            }

            DecorrelationResult childDecorrelationResult = childDecorrelationResultOptional.get();
            Set<Symbol> constantSymbols = childDecorrelationResult.getConstantSymbols();

            AggregationNode decorrelatedAggregation = childDecorrelationResult.getCorrelatedSymbolMapper()
                    .map(node, childDecorrelationResult.node);

            Set<Symbol> groupingKeys = ImmutableSet.copyOf(node.getGroupingKeys());
            List<Symbol> symbolsToAdd = childDecorrelationResult.symbolsToPropagate.stream()
                    .filter(symbol -> !groupingKeys.contains(symbol))
                    .collect(toImmutableList());

            if (!constantSymbols.containsAll(symbolsToAdd)) {
                return Optional.empty();
            }

            AggregationNode newAggregation = new AggregationNode(
                    decorrelatedAggregation.getId(),
                    decorrelatedAggregation.getSource(),
                    decorrelatedAggregation.getAggregations(),
                    AggregationNode.singleGroupingSet(ImmutableList.<Symbol>builder()
                            .addAll(node.getGroupingKeys())
                            .addAll(symbolsToAdd)
                            .build()),
                    ImmutableList.of(),
                    decorrelatedAggregation.getStep(),
                    decorrelatedAggregation.getHashSymbol(),
                    decorrelatedAggregation.getGroupIdSymbol());

            return Optional.of(new DecorrelationResult(
                    newAggregation,
                    childDecorrelationResult.symbolsToPropagate,
                    childDecorrelationResult.correlatedPredicates,
                    childDecorrelationResult.correlatedSymbolsMapping,
                    constantSymbols.containsAll(newAggregation.getGroupingKeys())));
        }

        @Override
        public Optional<DecorrelationResult> visitProject(ProjectNode node, Void context)
        {
            Optional<DecorrelationResult> childDecorrelationResultOptional = node.getSource().accept(this, null);
            if (!childDecorrelationResultOptional.isPresent()) {
                return Optional.empty();
            }

            DecorrelationResult childDecorrelationResult = childDecorrelationResultOptional.get();
            Set<Symbol> nodeOutputSymbols = ImmutableSet.copyOf(node.getOutputSymbols());
            List<Symbol> symbolsToAdd = childDecorrelationResult.symbolsToPropagate.stream()
                    .filter(symbol -> !nodeOutputSymbols.contains(symbol))
                    .collect(toImmutableList());

            Assignments assignments = Assignments.builder()
                    .putAll(node.getAssignments())
                    .putIdentities(symbolsToAdd)
                    .build();

            return Optional.of(new DecorrelationResult(
                    new ProjectNode(node.getId(), childDecorrelationResult.node, assignments),
                    childDecorrelationResult.symbolsToPropagate,
                    childDecorrelationResult.correlatedPredicates,
                    childDecorrelationResult.correlatedSymbolsMapping,
                    childDecorrelationResult.atMostSingleRow));
        }

        private Multimap<Symbol, Symbol> extractCorrelatedSymbolsMapping(List<Expression> correlatedConjuncts)
        {
            // TODO: handle coercions and non-direct column references
            ImmutableMultimap.Builder<Symbol, Symbol> mapping = ImmutableMultimap.builder();
            for (Expression conjunct : correlatedConjuncts) {
                if (!(conjunct instanceof ComparisonExpression)) {
                    continue;
                }

                ComparisonExpression comparison = (ComparisonExpression) conjunct;
                if (!(comparison.getLeft() instanceof SymbolReference
                        && comparison.getRight() instanceof SymbolReference
                        && comparison.getOperator().equals(EQUAL))) {
                    continue;
                }

                Symbol left = Symbol.from(comparison.getLeft());
                Symbol right = Symbol.from(comparison.getRight());

                if (correlation.contains(left) && !correlation.contains(right)) {
                    mapping.put(left, right);
                }

                if (correlation.contains(right) && !correlation.contains(left)) {
                    mapping.put(right, left);
                }
            }

            return mapping.build();
        }

        private boolean isCorrelated(Expression expression)
        {
            return correlation.stream().anyMatch(SymbolsExtractor.extractUnique(expression)::contains);
        }
    }

    private static class DecorrelationResult
    {
        final PlanNode node;
        final Set<Symbol> symbolsToPropagate;
        final List<Expression> correlatedPredicates;

        // mapping from correlated symbols to their uncorrelated equivalence
        final Multimap<Symbol, Symbol> correlatedSymbolsMapping;
        // If a subquery has at most single row for any correlation values?
        final boolean atMostSingleRow;

        DecorrelationResult(PlanNode node, Set<Symbol> symbolsToPropagate, List<Expression> correlatedPredicates, Multimap<Symbol, Symbol> correlatedSymbolsMapping, boolean atMostSingleRow)
        {
            this.node = node;
            this.symbolsToPropagate = symbolsToPropagate;
            this.correlatedPredicates = correlatedPredicates;
            this.atMostSingleRow = atMostSingleRow;
            this.correlatedSymbolsMapping = correlatedSymbolsMapping;
            checkState(symbolsToPropagate.containsAll(correlatedSymbolsMapping.values()), "Expected symbols to propagate to contain all constant symbols");
        }

        SymbolMapper getCorrelatedSymbolMapper()
        {
            return new SymbolMapper(correlatedSymbolsMapping.asMap().entrySet().stream()
                    .collect(toImmutableMap(Map.Entry::getKey, symbols -> Iterables.getLast(symbols.getValue()))));
        }

        /**
         * @return constant symbols from a perspective of a subquery
         */
        Set<Symbol> getConstantSymbols()
        {
            return ImmutableSet.copyOf(correlatedSymbolsMapping.values());
        }
    }

    private Optional<DecorrelatedNode> decorrelatedNode(
            List<Expression> correlatedPredicates,
            PlanNode node,
            List<Symbol> correlation)
    {
        if (containsCorrelation(node, correlation)) {
            // node is still correlated ; /
            return Optional.empty();
        }
        return Optional.of(new DecorrelatedNode(correlatedPredicates, node));
    }

    private boolean containsCorrelation(PlanNode node, List<Symbol> correlation)
    {
        return Sets.union(SymbolsExtractor.extractUnique(node, lookup), SymbolsExtractor.extractOutputSymbols(node, lookup)).stream().anyMatch(correlation::contains);
    }

    public static class DecorrelatedNode
    {
        private final List<Expression> correlatedPredicates;
        private final PlanNode node;

        public DecorrelatedNode(List<Expression> correlatedPredicates, PlanNode node)
        {
            requireNonNull(correlatedPredicates, "correlatedPredicates is null");
            this.correlatedPredicates = ImmutableList.copyOf(correlatedPredicates);
            this.node = requireNonNull(node, "node is null");
        }

        public Optional<Expression> getCorrelatedPredicates()
        {
            if (correlatedPredicates.isEmpty()) {
                return Optional.empty();
            }
            return Optional.of(ExpressionUtils.and(correlatedPredicates));
        }

        public PlanNode getNode()
        {
            return node;
        }
    }
}
