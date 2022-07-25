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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import io.trino.Session;
import io.trino.execution.warnings.WarningCollector;
import io.trino.metadata.Metadata;
import io.trino.spi.type.Type;
import io.trino.sql.planner.PlanNodeIdAllocator;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.SymbolAllocator;
import io.trino.sql.planner.TypeProvider;
import io.trino.sql.planner.plan.AggregationNode;
import io.trino.sql.planner.plan.AggregationNode.Aggregation;
import io.trino.sql.planner.plan.Assignments;
import io.trino.sql.planner.plan.GroupIdNode;
import io.trino.sql.planner.plan.MarkDistinctNode;
import io.trino.sql.planner.plan.PlanNode;
import io.trino.sql.planner.plan.ProjectNode;
import io.trino.sql.planner.plan.SimplePlanRewriter;
import io.trino.sql.tree.Cast;
import io.trino.sql.tree.CoalesceExpression;
import io.trino.sql.tree.ComparisonExpression;
import io.trino.sql.tree.Expression;
import io.trino.sql.tree.IfExpression;
import io.trino.sql.tree.LongLiteral;
import io.trino.sql.tree.NullLiteral;
import io.trino.sql.tree.QualifiedName;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.SystemSessionProperties.isOptimizeDistinctAggregationEnabled;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.sql.analyzer.TypeSignatureProvider.fromTypes;
import static io.trino.sql.analyzer.TypeSignatureTranslator.toSqlType;
import static io.trino.sql.planner.plan.AggregationNode.Step.SINGLE;
import static io.trino.sql.planner.plan.AggregationNode.singleGroupingSet;
import static java.util.Objects.requireNonNull;

/*
 * This optimizer convert query of form:
 *
 *  SELECT a1, a2,..., an, F1(b1), F2(b2), F3(b3), ...., Fm(bm), F(distinct c) FROM Table GROUP BY a1, a2, ..., an
 *
 *  INTO
 *
 *  SELECT a1, a2,..., an, arbitrary(if(group = 0, f1)),...., arbitrary(if(group = 0, fm)), F(if(group = 1, c)) FROM
 *      SELECT a1, a2,..., an, F1(b1) as f1, F2(b2) as f2,...., Fm(bm) as fm, c, group FROM
 *        SELECT a1, a2,..., an, b1, b2, ... ,bn, c FROM Table GROUP BY GROUPING SETS ((a1, a2,..., an, b1, b2, ... ,bn), (a1, a2,..., an, c))
 *      GROUP BY a1, a2,..., an, c, group
 *  GROUP BY a1, a2,..., an
 */
public class OptimizeMixedDistinctAggregations
        implements PlanOptimizer
{
    private final Metadata metadata;

    public OptimizeMixedDistinctAggregations(Metadata metadata)
    {
        this.metadata = metadata;
    }

    @Override
    public PlanNode optimize(PlanNode plan, Session session, TypeProvider types, SymbolAllocator symbolAllocator, PlanNodeIdAllocator idAllocator, WarningCollector warningCollector)
    {
        if (isOptimizeDistinctAggregationEnabled(session)) {
            return SimplePlanRewriter.rewriteWith(new Optimizer(session, idAllocator, symbolAllocator, metadata), plan, Optional.empty());
        }

        return plan;
    }

    private static class Optimizer
            extends SimplePlanRewriter<Optional<AggregateInfo>>
    {
        private final Session session;
        private final PlanNodeIdAllocator idAllocator;
        private final SymbolAllocator symbolAllocator;
        private final Metadata metadata;

        private Optimizer(Session session, PlanNodeIdAllocator idAllocator, SymbolAllocator symbolAllocator, Metadata metadata)
        {
            this.session = requireNonNull(session, "session is null");
            this.idAllocator = requireNonNull(idAllocator, "idAllocator is null");
            this.symbolAllocator = requireNonNull(symbolAllocator, "symbolAllocator is null");
            this.metadata = requireNonNull(metadata, "metadata is null");
        }

        @Override
        public PlanNode visitAggregation(AggregationNode node, RewriteContext<Optional<AggregateInfo>> context)
        {
            // optimize if and only if
            // some aggregation functions have a distinct mask symbol
            // and if not all aggregation functions on same distinct mask symbol (this case handled by SingleDistinctOptimizer)
            List<Symbol> masks = node.getAggregations().values().stream()
                    .map(Aggregation::getMask).filter(Optional::isPresent).map(Optional::get).collect(toImmutableList());
            Set<Symbol> uniqueMasks = ImmutableSet.copyOf(masks);
            if (uniqueMasks.size() != 1 || masks.size() == node.getAggregations().size()) {
                return context.defaultRewrite(node, Optional.empty());
            }

            if (node.getAggregations().values().stream().map(Aggregation::getFilter).anyMatch(Optional::isPresent)) {
                // Skip if any aggregation contains a filter
                return context.defaultRewrite(node, Optional.empty());
            }

            if (node.hasOrderings()) {
                // Skip if any aggregation contains a order by
                return context.defaultRewrite(node, Optional.empty());
            }

            AggregateInfo aggregateInfo = new AggregateInfo(
                    node.getGroupingKeys(),
                    Iterables.getOnlyElement(uniqueMasks),
                    node.getAggregations());

            if (!checkAllEquatableTypes(aggregateInfo)) {
                // This optimization relies on being able to GROUP BY arguments
                // of the original aggregation functions. If they their types are
                // not comparable, we have to skip it.
                return context.defaultRewrite(node, Optional.empty());
            }

            PlanNode source = context.rewrite(node.getSource(), Optional.of(aggregateInfo));

            // make sure there's a markdistinct associated with this aggregation
            if (!aggregateInfo.isFoundMarkDistinct()) {
                return context.defaultRewrite(node, Optional.empty());
            }

            // Change aggregate node to do second aggregation, handles this part of optimized plan mentioned above:
            //          SELECT a1, a2,..., an, arbitrary(if(group = 0, f1)),...., arbitrary(if(group = 0, fm)), F(if(group = 1, c))
            ImmutableMap.Builder<Symbol, Aggregation> aggregations = ImmutableMap.builder();
            // Add coalesce projection node to handle count(), count_if(), approx_distinct() functions return a
            // non-null result without any input
            ImmutableMap.Builder<Symbol, Symbol> coalesceSymbolsBuilder = ImmutableMap.builder();
            for (Map.Entry<Symbol, Aggregation> entry : node.getAggregations().entrySet()) {
                Aggregation aggregation = entry.getValue();
                if (aggregation.getMask().isPresent()) {
                    aggregations.put(entry.getKey(), new Aggregation(
                            aggregation.getResolvedFunction(),
                            ImmutableList.of(aggregateInfo.getNewDistinctAggregateSymbol().toSymbolReference()),
                            false,
                            Optional.empty(),
                            Optional.empty(),
                            Optional.empty()));
                }
                else {
                    // Aggregations on non-distinct are already done by new node, just extract the non-null value
                    Symbol argument = aggregateInfo.getNewNonDistinctAggregateSymbols().get(entry.getKey());
                    QualifiedName functionName = QualifiedName.of("arbitrary");
                    Aggregation newAggregation = new Aggregation(
                            metadata.resolveFunction(session, functionName, fromTypes(symbolAllocator.getTypes().get(argument))),
                            ImmutableList.of(argument.toSymbolReference()),
                            false,
                            Optional.empty(),
                            Optional.empty(),
                            Optional.empty());
                    String signatureName = aggregation.getResolvedFunction().getSignature().getName();
                    if (signatureName.equals("count") || signatureName.equals("count_if") || signatureName.equals("approx_distinct")) {
                        Symbol newSymbol = symbolAllocator.newSymbol("expr", symbolAllocator.getTypes().get(entry.getKey()));
                        aggregations.put(newSymbol, newAggregation);
                        coalesceSymbolsBuilder.put(newSymbol, entry.getKey());
                    }
                    else {
                        aggregations.put(entry.getKey(), newAggregation);
                    }
                }
            }
            Map<Symbol, Symbol> coalesceSymbols = coalesceSymbolsBuilder.buildOrThrow();

            AggregationNode aggregationNode = new AggregationNode(
                    idAllocator.getNextId(),
                    source,
                    aggregations.buildOrThrow(),
                    node.getGroupingSets(),
                    ImmutableList.of(),
                    node.getStep(),
                    Optional.empty(),
                    node.getGroupIdSymbol());

            if (coalesceSymbols.isEmpty()) {
                return aggregationNode;
            }

            Assignments.Builder outputSymbols = Assignments.builder();
            for (Symbol symbol : aggregationNode.getOutputSymbols()) {
                if (coalesceSymbols.containsKey(symbol)) {
                    Expression expression = new CoalesceExpression(symbol.toSymbolReference(), new Cast(new LongLiteral("0"), toSqlType(BIGINT)));
                    outputSymbols.put(coalesceSymbols.get(symbol), expression);
                }
                else {
                    outputSymbols.putIdentity(symbol);
                }
            }

            return new ProjectNode(idAllocator.getNextId(), aggregationNode, outputSymbols.build());
        }

        @Override
        public PlanNode visitMarkDistinct(MarkDistinctNode node, RewriteContext<Optional<AggregateInfo>> context)
        {
            Optional<AggregateInfo> aggregateInfo = context.get();

            // presence of aggregateInfo => mask also present
            if (aggregateInfo.isEmpty() || !aggregateInfo.get().getMask().equals(node.getMarkerSymbol())) {
                return context.defaultRewrite(node, Optional.empty());
            }

            aggregateInfo.get().foundMarkDistinct();

            PlanNode source = context.rewrite(node.getSource(), Optional.empty());

            Set<Symbol> allSymbols = new HashSet<>();
            List<Symbol> groupBySymbols = aggregateInfo.get().getGroupBySymbols(); // a
            List<Symbol> nonDistinctAggregateSymbols = aggregateInfo.get().getOriginalNonDistinctAggregateArgs(); //b
            Symbol distinctSymbol = Iterables.getOnlyElement(aggregateInfo.get().getOriginalDistinctAggregateArgs()); // c

            // If same symbol present in aggregations on distinct and non-distinct values, e.g. select sum(a), count(distinct a),
            // then we need to create a duplicate stream for this symbol
            Symbol duplicatedDistinctSymbol = distinctSymbol;

            if (nonDistinctAggregateSymbols.contains(distinctSymbol)) {
                Symbol newSymbol = symbolAllocator.newSymbol(distinctSymbol.getName(), symbolAllocator.getTypes().get(distinctSymbol));
                nonDistinctAggregateSymbols.set(nonDistinctAggregateSymbols.indexOf(distinctSymbol), newSymbol);
                duplicatedDistinctSymbol = newSymbol;
            }

            allSymbols.addAll(groupBySymbols);
            allSymbols.addAll(nonDistinctAggregateSymbols);
            allSymbols.add(distinctSymbol);

            // 1. Add GroupIdNode
            Symbol groupSymbol = symbolAllocator.newSymbol("group", BIGINT); // g
            GroupIdNode groupIdNode = createGroupIdNode(
                    groupBySymbols,
                    nonDistinctAggregateSymbols,
                    distinctSymbol,
                    duplicatedDistinctSymbol,
                    groupSymbol,
                    allSymbols,
                    source);

            // 2. Add aggregation node
            Set<Symbol> groupByKeys = new HashSet<>(groupBySymbols);
            groupByKeys.add(distinctSymbol);
            groupByKeys.add(groupSymbol);

            ImmutableMap.Builder<Symbol, Symbol> aggregationOutputSymbolsMapBuilder = ImmutableMap.builder();
            AggregationNode aggregationNode = createNonDistinctAggregation(
                    aggregateInfo.get(),
                    distinctSymbol,
                    duplicatedDistinctSymbol,
                    groupByKeys,
                    groupIdNode,
                    node,
                    aggregationOutputSymbolsMapBuilder);
            // This map has mapping only for aggregation on non-distinct symbols which the new AggregationNode handles
            Map<Symbol, Symbol> aggregationOutputSymbolsMap = aggregationOutputSymbolsMapBuilder.buildOrThrow();

            // 3. Add new project node that adds if expressions
            ProjectNode projectNode = createProjectNode(
                    aggregationNode,
                    aggregateInfo.get(),
                    distinctSymbol,
                    groupSymbol,
                    groupBySymbols,
                    aggregationOutputSymbolsMap);

            return projectNode;
        }

        // Returns false if either mask symbol or any of the symbols in aggregations is not comparable
        private boolean checkAllEquatableTypes(AggregateInfo aggregateInfo)
        {
            for (Symbol symbol : aggregateInfo.getOriginalNonDistinctAggregateArgs()) {
                Type type = symbolAllocator.getTypes().get(symbol);
                if (!type.isComparable()) {
                    return false;
                }
            }

            return symbolAllocator.getTypes().get(aggregateInfo.getMask()).isComparable();
        }

        /*
         * This Project is useful for cases when we aggregate on distinct and non-distinct values of same symbol, eg:
         *  select a, sum(b), count(c), sum(distinct c) group by a
         * Without this Project, we would count additional values for count(c)
         *
         * This method also populates maps of old to new symbols. For each key of outputNonDistinctAggregateSymbols,
         * Higher level aggregation node's aggregation <key, AggregateExpression> will now have to run AggregateExpression on value of outputNonDistinctAggregateSymbols
         * Same for outputDistinctAggregateSymbols map
         */
        private ProjectNode createProjectNode(
                AggregationNode source,
                AggregateInfo aggregateInfo,
                Symbol distinctSymbol,
                Symbol groupSymbol,
                List<Symbol> groupBySymbols,
                Map<Symbol, Symbol> aggregationOutputSymbolsMap)
        {
            Assignments.Builder outputSymbols = Assignments.builder();
            ImmutableMap.Builder<Symbol, Symbol> outputNonDistinctAggregateSymbols = ImmutableMap.builder();
            for (Symbol symbol : source.getOutputSymbols()) {
                if (distinctSymbol.equals(symbol)) {
                    Symbol newSymbol = symbolAllocator.newSymbol("expr", symbolAllocator.getTypes().get(symbol));
                    aggregateInfo.setNewDistinctAggregateSymbol(newSymbol);

                    Expression expression = createIfExpression(
                            groupSymbol.toSymbolReference(),
                            new Cast(new LongLiteral("1"), toSqlType(BIGINT)), // TODO: this should use GROUPING() when that's available instead of relying on specific group numbering
                            ComparisonExpression.Operator.EQUAL,
                            symbol.toSymbolReference(),
                            symbolAllocator.getTypes().get(symbol));
                    outputSymbols.put(newSymbol, expression);
                }
                else if (aggregationOutputSymbolsMap.containsKey(symbol)) {
                    Symbol newSymbol = symbolAllocator.newSymbol("expr", symbolAllocator.getTypes().get(symbol));
                    // key of outputNonDistinctAggregateSymbols is key of an aggregation in AggrNode above, it will now aggregate on this Map's value
                    outputNonDistinctAggregateSymbols.put(aggregationOutputSymbolsMap.get(symbol), newSymbol);
                    Expression expression = createIfExpression(
                            groupSymbol.toSymbolReference(),
                            new Cast(new LongLiteral("0"), toSqlType(BIGINT)), // TODO: this should use GROUPING() when that's available instead of relying on specific group numbering
                            ComparisonExpression.Operator.EQUAL,
                            symbol.toSymbolReference(),
                            symbolAllocator.getTypes().get(symbol));
                    outputSymbols.put(newSymbol, expression);
                }

                // A symbol can appear both in groupBy and distinct/non-distinct aggregation
                if (groupBySymbols.contains(symbol)) {
                    Expression expression = symbol.toSymbolReference();
                    outputSymbols.put(symbol, expression);
                }
            }

            // add null assignment for mask
            // unused mask will be removed by PruneUnreferencedOutputs
            outputSymbols.put(aggregateInfo.getMask(), new NullLiteral());

            aggregateInfo.setNewNonDistinctAggregateSymbols(outputNonDistinctAggregateSymbols.buildOrThrow());

            return new ProjectNode(idAllocator.getNextId(), source, outputSymbols.build());
        }

        private GroupIdNode createGroupIdNode(
                List<Symbol> groupBySymbols,
                List<Symbol> nonDistinctAggregateSymbols,
                Symbol distinctSymbol,
                Symbol duplicatedDistinctSymbol,
                Symbol groupSymbol,
                Set<Symbol> allSymbols,
                PlanNode source)
        {
            List<List<Symbol>> groups = new ArrayList<>();
            // g0 = {group-by symbols + allNonDistinctAggregateSymbols}
            // g1 = {group-by symbols + Distinct Symbol}
            // symbols present in Group_i will be set, rest will be Null

            //g0
            Set<Symbol> group0 = new HashSet<>();
            group0.addAll(groupBySymbols);
            group0.addAll(nonDistinctAggregateSymbols);
            groups.add(ImmutableList.copyOf(group0));

            // g1
            Set<Symbol> group1 = new HashSet<>(groupBySymbols);
            group1.add(distinctSymbol);
            groups.add(ImmutableList.copyOf(group1));

            return new GroupIdNode(
                    idAllocator.getNextId(),
                    source,
                    groups,
                    allSymbols.stream().collect(Collectors.toMap(
                            symbol -> symbol,
                            symbol -> (symbol.equals(duplicatedDistinctSymbol) ? distinctSymbol : symbol))),
                    ImmutableList.of(),
                    groupSymbol);
        }

        /*
         * This method returns a new Aggregation node which has aggregations on non-distinct symbols from original plan. Generates
         *      SELECT a1, a2,..., an, F1(b1) as f1, F2(b2) as f2,...., Fm(bm) as fm, c, group
         * part in the optimized plan mentioned above
         *
         * It also populates the mappings of new function's output symbol to corresponding old function's output symbol, e.g.
         *     { f1 -> F1, f2 -> F2, ... }
         * The new AggregateNode aggregates on the symbols that original AggregationNode aggregated on
         * Original one will now aggregate on the output symbols of this new node
         */
        private AggregationNode createNonDistinctAggregation(
                AggregateInfo aggregateInfo,
                Symbol distinctSymbol,
                Symbol duplicatedDistinctSymbol,
                Set<Symbol> groupByKeys,
                GroupIdNode groupIdNode,
                MarkDistinctNode originalNode,
                ImmutableMap.Builder<Symbol, Symbol> aggregationOutputSymbolsMapBuilder)
        {
            ImmutableMap.Builder<Symbol, Aggregation> aggregations = ImmutableMap.builder();
            for (Map.Entry<Symbol, Aggregation> entry : aggregateInfo.getAggregations().entrySet()) {
                Aggregation aggregation = entry.getValue();
                if (aggregation.getMask().isEmpty()) {
                    Symbol newSymbol = symbolAllocator.newSymbol(entry.getKey().toSymbolReference(), symbolAllocator.getTypes().get(entry.getKey()));
                    aggregationOutputSymbolsMapBuilder.put(newSymbol, entry.getKey());
                    if (!duplicatedDistinctSymbol.equals(distinctSymbol)) {
                        // Handling for cases when mask symbol appears in non distinct aggregations too
                        // Now the aggregation should happen over the duplicate symbol added before
                        if (aggregation.getArguments().contains(distinctSymbol.toSymbolReference())) {
                            ImmutableList.Builder<Expression> arguments = ImmutableList.builder();
                            for (Expression argument : aggregation.getArguments()) {
                                if (distinctSymbol.toSymbolReference().equals(argument)) {
                                    arguments.add(duplicatedDistinctSymbol.toSymbolReference());
                                }
                                else {
                                    arguments.add(argument);
                                }
                            }
                            aggregation = new Aggregation(
                                    aggregation.getResolvedFunction(),
                                    arguments.build(),
                                    false,
                                    Optional.empty(),
                                    Optional.empty(),
                                    Optional.empty());
                        }
                    }
                    aggregations.put(newSymbol, aggregation);
                }
            }
            return new AggregationNode(
                    idAllocator.getNextId(),
                    groupIdNode,
                    aggregations.buildOrThrow(),
                    singleGroupingSet(ImmutableList.copyOf(groupByKeys)),
                    ImmutableList.of(),
                    SINGLE,
                    originalNode.getHashSymbol(),
                    Optional.empty());
        }

        // creates if clause specific to use case here, default value always null
        private static IfExpression createIfExpression(Expression left, Expression right, ComparisonExpression.Operator operator, Expression result, Type trueValueType)
        {
            return new IfExpression(
                    new ComparisonExpression(operator, left, right),
                    result,
                    new Cast(new NullLiteral(), toSqlType(trueValueType)));
        }
    }

    private static class AggregateInfo
    {
        private final List<Symbol> groupBySymbols;
        private final Symbol mask;
        private final Map<Symbol, Aggregation> aggregations;

        // Filled on the way back, these are the symbols corresponding to their distinct or non-distinct original symbols
        private Map<Symbol, Symbol> newNonDistinctAggregateSymbols;
        private Symbol newDistinctAggregateSymbol;
        private boolean foundMarkDistinct;

        public AggregateInfo(List<Symbol> groupBySymbols, Symbol mask, Map<Symbol, Aggregation> aggregations)
        {
            this.groupBySymbols = ImmutableList.copyOf(groupBySymbols);

            this.mask = mask;

            this.aggregations = ImmutableMap.copyOf(aggregations);
        }

        public List<Symbol> getOriginalNonDistinctAggregateArgs()
        {
            return aggregations.values().stream()
                    .filter(aggregation -> aggregation.getMask().isEmpty())
                    .flatMap(aggregation -> aggregation.getArguments().stream())
                    .distinct()
                    .map(Symbol::from)
                    .collect(Collectors.toList());
        }

        public List<Symbol> getOriginalDistinctAggregateArgs()
        {
            return aggregations.values().stream()
                    .filter(aggregation -> aggregation.getMask().isPresent())
                    .flatMap(aggregation -> aggregation.getArguments().stream())
                    .distinct()
                    .map(Symbol::from)
                    .collect(Collectors.toList());
        }

        public Symbol getNewDistinctAggregateSymbol()
        {
            return newDistinctAggregateSymbol;
        }

        public void setNewDistinctAggregateSymbol(Symbol newDistinctAggregateSymbol)
        {
            this.newDistinctAggregateSymbol = newDistinctAggregateSymbol;
        }

        public Map<Symbol, Symbol> getNewNonDistinctAggregateSymbols()
        {
            return newNonDistinctAggregateSymbols;
        }

        public void setNewNonDistinctAggregateSymbols(Map<Symbol, Symbol> newNonDistinctAggregateSymbols)
        {
            this.newNonDistinctAggregateSymbols = newNonDistinctAggregateSymbols;
        }

        public Symbol getMask()
        {
            return mask;
        }

        public List<Symbol> getGroupBySymbols()
        {
            return groupBySymbols;
        }

        public Map<Symbol, Aggregation> getAggregations()
        {
            return aggregations;
        }

        public void foundMarkDistinct()
        {
            foundMarkDistinct = true;
        }

        public boolean isFoundMarkDistinct()
        {
            return foundMarkDistinct;
        }
    }
}
