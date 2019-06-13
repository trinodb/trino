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
package io.prestosql.sql.planner.iterative.rule;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.prestosql.matching.Captures;
import io.prestosql.matching.Pattern;
import io.prestosql.metadata.Metadata;
import io.prestosql.spi.type.BigintType;
import io.prestosql.spi.type.Type;
import io.prestosql.sql.analyzer.TypeSignatureProvider;
import io.prestosql.sql.planner.Symbol;
import io.prestosql.sql.planner.SymbolAllocator;
import io.prestosql.sql.planner.iterative.Rule;
import io.prestosql.sql.planner.plan.AggregationNode;
import io.prestosql.sql.planner.plan.AggregationNode.Aggregation;
import io.prestosql.sql.planner.plan.Assignments;
import io.prestosql.sql.planner.plan.GroupIdNode;
import io.prestosql.sql.planner.plan.PlanNode;
import io.prestosql.sql.planner.plan.ProjectNode;
import io.prestosql.sql.tree.Cast;
import io.prestosql.sql.tree.CoalesceExpression;
import io.prestosql.sql.tree.ComparisonExpression;
import io.prestosql.sql.tree.Expression;
import io.prestosql.sql.tree.FunctionCall;
import io.prestosql.sql.tree.IfExpression;
import io.prestosql.sql.tree.LongLiteral;
import io.prestosql.sql.tree.NullLiteral;
import io.prestosql.sql.tree.QualifiedName;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static io.prestosql.SystemSessionProperties.isOptimizeDistinctAggregationEnabled;
import static io.prestosql.sql.planner.plan.AggregationNode.Step.SINGLE;
import static io.prestosql.sql.planner.plan.AggregationNode.singleGroupingSet;
import static io.prestosql.sql.planner.plan.Patterns.aggregation;
import static java.util.Objects.requireNonNull;

/*
 * This optimizer rule convert query of form:
 *
 *  SELECT a1, a2,..., an, F1(b1), F2(b2), F3(b3), ...., Fm(bm), F1(distinct c1), ...., Fm(distinct cm) FROM Table GROUP BY a1, a2, ..., an
 *
 *  INTO
 *
 *  SELECT a1, a2,..., an, arbitrary(if(group = 0, f1)),...., arbitrary(if(group = 0, fm)), F(if(group = 1, c1)), ...., F(if(group = m, cm)) FROM
 *      SELECT a1, a2,..., an, F1(b1) as f1, F2(b2) as f2,...., Fm(bm) as fm, c1,..., cm group FROM
 *        SELECT a1, a2,..., an, b1, b2, ... ,bn, c1,..., cm FROM Table GROUP BY GROUPING SETS ((a1, a2,..., an, b1, b2, ... ,bn), (a1, a2,..., an, c1), ..., ((a1, a2,..., an, cm)))
 *      GROUP BY a1, a2,..., an, c1,..., cm group
 *  GROUP BY a1, a2,..., an
 */

public class DistinctAggregationToGroupBy
        implements Rule<AggregationNode>
{
    private static final Pattern<AggregationNode> PATTERN = aggregation()
            .matching(DistinctAggregationToGroupBy::hasDistinctInput)
            .matching(DistinctAggregationToGroupBy::hasSingleDistinctArgument)
            .matching(DistinctAggregationToGroupBy::noFilters)
            .matching(DistinctAggregationToGroupBy::noMasks)
            .matching(DistinctAggregationToGroupBy::noOrdering);

    private static boolean hasDistinctInput(AggregationNode aggregation)
    {
        return aggregation.getAggregations()
                .values().stream()
                .map(Aggregation::getCall)
                .filter(FunctionCall::isDistinct)
                .map(FunctionCall::getArguments)
                .<Set<Expression>>map(HashSet::new)
                .distinct()
                .count() > 0;
    }

    private static boolean hasSingleDistinctArgument(AggregationNode aggregation)
    {
        return aggregation.getAggregations()
                .values().stream()
                .map(Aggregation::getCall)
                .filter(FunctionCall::isDistinct)
                .allMatch(c -> c.getArguments().size() == 1);
    }

    private static boolean noFilters(AggregationNode aggregation)
    {
        return aggregation.getAggregations()
                .values().stream()
                .map(Aggregation::getCall)
                .noneMatch(call -> call.getFilter().isPresent());
    }

    private static boolean noMasks(AggregationNode aggregation)
    {
        return aggregation.getAggregations()
                .values().stream()
                .noneMatch(e -> e.getMask().isPresent());
    }

    private static boolean noOrdering(AggregationNode aggregation)
    {
        return !aggregation.hasOrderings();
    }

    private final Metadata metadata;

    public DistinctAggregationToGroupBy(Metadata metadata)
    {
        this.metadata = requireNonNull(metadata, "metadata is null");
    }

    @Override
    public Pattern<AggregationNode> getPattern()
    {
        return PATTERN;
    }

    @Override
    public Result apply(AggregationNode node, Captures captures, Context context)
    {
        if (!isOptimizeDistinctAggregationEnabled(context.getSession())) {
            return Result.empty();
        }

        SymbolAllocator symbolAllocator = context.getSymbolAllocator();
        Set<Symbol> allSymbols = new HashSet<>();
        List<Symbol> groupBySymbols = node.getGroupingKeys();
        List<Symbol> distinctSymbols = node.getAggregations().values().stream()
                .map(Aggregation::getCall)
                .filter(FunctionCall::isDistinct)
                .flatMap(function -> function.getArguments().stream())
                .distinct()
                .map(Symbol::from)
                .collect(Collectors.toList());

        // If same symbol present in aggregations on distinct and non-distinct values, e.g. select sum(a), count(distinct a),
        // then we need to create a duplicate stream for this symbol
        ImmutableMap.Builder<Symbol, Symbol> duplicatedDistinctSymbolMapBuilder = ImmutableMap.builder(); // For groupIdNode groupingColumns
        ImmutableList.Builder<Symbol> nonDistinctAggregateSymbolsBuilder = ImmutableList.builder(); // For groupIdNode groupingSets
        ImmutableMap.Builder<FunctionCall, FunctionCall> nonDistinctAggrFunctionCallMapMapBuilder = ImmutableMap.builder(); // For first aggregation node

        for (Map.Entry<Symbol, Aggregation> entry : node.getAggregations().entrySet()) {
            FunctionCall functionCall = entry.getValue().getCall();
            if (!functionCall.isDistinct()) {
                ImmutableList.Builder<Expression> arguments = ImmutableList.builder();
                for (Expression argument : functionCall.getArguments()) {
                    Symbol symbol = Symbol.from(argument);
                    if (distinctSymbols.contains(symbol)) {
                        Symbol newSymbol = symbolAllocator.newSymbol(symbol.getName(), symbolAllocator.getTypes().get(symbol));
                        duplicatedDistinctSymbolMapBuilder.put(newSymbol, symbol);
                        arguments.add(newSymbol.toSymbolReference());
                        nonDistinctAggregateSymbolsBuilder.add(newSymbol);
                    }
                    else {
                        arguments.add(argument);
                        nonDistinctAggregateSymbolsBuilder.add(symbol);
                    }
                }
                FunctionCall newFunctionCall = new FunctionCall(functionCall.getName(), functionCall.getWindow(), false, arguments.build());
                nonDistinctAggrFunctionCallMapMapBuilder.put(functionCall, newFunctionCall);
            }
        }

        List<Symbol> nonDistinctAggregateSymbols = nonDistinctAggregateSymbolsBuilder.build().stream().distinct().collect(Collectors.toList());
        allSymbols.addAll(groupBySymbols);
        allSymbols.addAll(nonDistinctAggregateSymbols);
        allSymbols.addAll(distinctSymbols);

        // 1. Add GroupIdNode
        ImmutableMap.Builder<Symbol, Integer> symbolGroupNumMapBuilder = ImmutableMap.builder();
        Symbol groupSymbol = symbolAllocator.newSymbol("group", BigintType.BIGINT); // g
        GroupIdNode groupIdNode = createGroupIdNode(
                groupBySymbols,
                nonDistinctAggregateSymbols,
                distinctSymbols,
                duplicatedDistinctSymbolMapBuilder.build(),
                groupSymbol,
                allSymbols,
                node.getSource(),
                context,
                symbolGroupNumMapBuilder);

        // 2. Add aggregation node
        Set<Symbol> groupByKeys = new HashSet<>(groupBySymbols);
        groupByKeys.addAll(distinctSymbols);
        groupByKeys.add(groupSymbol);

        ImmutableMap.Builder<Symbol, Symbol> aggregationOutputSymbolsMapBuilder = ImmutableMap.builder(); // For projection node
        AggregationNode nonDistinctAggregationNode = createNonDistinctAggregation(
                node,
                context,
                groupByKeys,
                groupIdNode,
                nonDistinctAggrFunctionCallMapMapBuilder.build(),
                aggregationOutputSymbolsMapBuilder);
        // This map has mapping only for aggregation on non-distinct symbols which the new AggregationNode handles
        Map<Symbol, Symbol> aggregationOutputSymbolsMap = aggregationOutputSymbolsMapBuilder.build();

        // 3. Add new project node that adds if expressions
        ImmutableMap.Builder<Symbol, Symbol> outputNonDistinctAggregateSymbolsMapBuilder = ImmutableMap.builder();
        ImmutableMap.Builder<Symbol, Symbol> outputDistinctAggregateSymbolsMapBuilder = ImmutableMap.builder();
        ProjectNode projectNode = createProjectNode(
                context,
                nonDistinctAggregationNode,
                distinctSymbols,
                groupSymbol,
                groupBySymbols,
                aggregationOutputSymbolsMap,
                outputNonDistinctAggregateSymbolsMapBuilder,
                outputDistinctAggregateSymbolsMapBuilder,
                symbolGroupNumMapBuilder.build());

        // 4. Change aggregate node to do second aggregation, handles this part of optimized plan mentioned above:
        //          SELECT a1, a2,..., an, arbitrary(if(group = 0, f1)),...., arbitrary(if(group = 0, fm)), F1(if(group = 1, c)),...., Fm(if(group = 1, c))
        Map<Symbol, Symbol> outputNonDistinctAggregateSymbolsMap = outputNonDistinctAggregateSymbolsMapBuilder.build();
        Map<Symbol, Symbol> outputDistinctAggregateSymbolsMap = outputDistinctAggregateSymbolsMapBuilder.build();
        ImmutableMap.Builder<Symbol, Aggregation> aggregations = ImmutableMap.builder();
        // Add coalesce projection node to handle count(), count_if(), approx_distinct() functions return a
        // non-null result without any input
        ImmutableMap.Builder<Symbol, Symbol> coalesceSymbolsBuilder = ImmutableMap.builder();

        for (Map.Entry<Symbol, Aggregation> entry : node.getAggregations().entrySet()) {
            FunctionCall functionCall = entry.getValue().getCall();
            if (functionCall.isDistinct()) {
                Symbol argument = outputDistinctAggregateSymbolsMap.get(Symbol.from(entry.getValue().getCall().getArguments().get(0)));
                aggregations.put(entry.getKey(), new Aggregation(
                        new FunctionCall(
                                functionCall.getName(),
                                functionCall.getWindow(),
                                false,
                                ImmutableList.of(argument.toSymbolReference())),
                        entry.getValue().getSignature(),
                        Optional.empty()));
            }
            else {
                // Aggregations on non-distinct are already done by new node, just extract the non-null value
                Symbol argument = outputNonDistinctAggregateSymbolsMap.get(entry.getKey());
                QualifiedName functionName = QualifiedName.of("arbitrary");
                String signatureName = entry.getValue().getSignature().getName();
                Aggregation aggregation = new Aggregation(
                        new FunctionCall(functionName, functionCall.getWindow(), false, ImmutableList.of(argument.toSymbolReference())),
                        metadata.getFunctionRegistry()
                                .resolveFunction(
                                        functionName,
                                        ImmutableList.of(new TypeSignatureProvider(symbolAllocator.getTypes().get(argument).getTypeSignature()))),
                        Optional.empty());

                if (signatureName.equals("count")
                        || signatureName.equals("count_if") || signatureName.equals("approx_distinct")) {
                    Symbol newSymbol = symbolAllocator.newSymbol("expr", symbolAllocator.getTypes().get(entry.getKey()));
                    aggregations.put(newSymbol, aggregation);
                    coalesceSymbolsBuilder.put(newSymbol, entry.getKey());
                }
                else {
                    aggregations.put(entry.getKey(), aggregation);
                }
            }
        }

        Map<Symbol, Symbol> coalesceSymbols = coalesceSymbolsBuilder.build();

        AggregationNode aggregationNode = new AggregationNode(
                context.getIdAllocator().getNextId(),
                projectNode,
                aggregations.build(),
                node.getGroupingSets(),
                ImmutableList.of(),
                node.getStep(),
                Optional.empty(),
                node.getGroupIdSymbol());

        if (coalesceSymbols.isEmpty()) {
            return Result.ofPlanNode(aggregationNode);
        }

        Assignments.Builder outputSymbols = Assignments.builder();
        for (Symbol symbol : aggregationNode.getOutputSymbols()) {
            if (coalesceSymbols.containsKey(symbol)) {
                Expression expression = new CoalesceExpression(symbol.toSymbolReference(), new Cast(new LongLiteral("0"), "bigint"));
                outputSymbols.put(coalesceSymbols.get(symbol), expression);
            }
            else {
                outputSymbols.putIdentity(symbol);
            }
        }

        return Result.ofPlanNode(new ProjectNode(context.getIdAllocator().getNextId(), aggregationNode, outputSymbols.build()));
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
            Context context,
            AggregationNode source,
            List<Symbol> distinctSymbols,
            Symbol groupSymbol,
            List<Symbol> groupBySymbols,
            Map<Symbol, Symbol> aggregationOutputSymbolsMap,
            ImmutableMap.Builder<Symbol, Symbol> outputNonDistinctAggregateSymbols,
            ImmutableMap.Builder<Symbol, Symbol> outputDistinctAggregateSymbols,
            Map<Symbol, Integer> symbolGroupNumMap)
    {
        Assignments.Builder outputSymbols = Assignments.builder();

        SymbolAllocator symbolAllocator = context.getSymbolAllocator();
        for (Symbol symbol : source.getOutputSymbols()) {
            if (distinctSymbols.contains(symbol)) {
                Symbol newSymbol = symbolAllocator.newSymbol("expr", symbolAllocator.getTypes().get(symbol));
                outputDistinctAggregateSymbols.put(symbol, newSymbol);

                Expression expression = createIfExpression(
                        groupSymbol.toSymbolReference(),
                        new Cast(new LongLiteral(symbolGroupNumMap.get(symbol).toString()), "bigint"), // TODO: this should use GROUPING() when that's available instead of relying on specific group numbering
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
                        new Cast(new LongLiteral("0"), "bigint"), // TODO: this should use GROUPING() when that's available instead of relying on specific group numbering
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

        return new ProjectNode(context.getIdAllocator().getNextId(), source, outputSymbols.build());
    }

    // creates if clause specific to use case here, default value always null
    private IfExpression createIfExpression(Expression left, Expression right, ComparisonExpression.Operator operator, Expression result, Type trueValueType)
    {
        return new IfExpression(
                new ComparisonExpression(operator, left, right),
                result,
                new Cast(new NullLiteral(), trueValueType.getTypeSignature().toString()));
    }

    private GroupIdNode createGroupIdNode(
            List<Symbol> groupBySymbols,
            List<Symbol> nonDistinctAggregateSymbols,
            List<Symbol> distinctSymbols,
            Map<Symbol, Symbol> duplicatedDistinctSymbols,
            Symbol groupSymbol,
            Set<Symbol> allSymbols,
            PlanNode source,
            Context context,
            ImmutableMap.Builder<Symbol, Integer> symbolGroupNumMap)
    {
        List<List<Symbol>> groups = new ArrayList<>();
        // g0 = {group-by symbols + allNonDistinctAggregateSymbols}
        // g1 to gn = {group-by symbols + Distinct Symbol}
        // symbols present in Group_i will be set, rest will be Null

        // g0
        Set<Symbol> group0 = new HashSet<>();
        group0.addAll(groupBySymbols);
        group0.addAll(nonDistinctAggregateSymbols);
        groups.add(ImmutableList.copyOf(group0));

        // g1 to gn
        for (int i = 0; i < distinctSymbols.size(); ++i) {
            Symbol distinctSymbol = distinctSymbols.get(i);
            Set<Symbol> distinctGroup = new HashSet<>();
            distinctGroup.addAll(groupBySymbols);
            distinctGroup.add(distinctSymbol);
            groups.add(ImmutableList.copyOf(distinctGroup));
            symbolGroupNumMap.put(distinctSymbol, i + 1);
        }

        return new GroupIdNode(
                context.getIdAllocator().getNextId(),
                source,
                groups,
                allSymbols.stream().collect(Collectors.toMap(
                        symbol -> symbol,
                        symbol -> ((duplicatedDistinctSymbols.containsKey(symbol)) ? duplicatedDistinctSymbols.get(symbol) : symbol))),
                ImmutableList.of(),
                groupSymbol);
    }

    /*
     * This method returns a new Aggregation node which has aggregations on non-distinct symbols from original plan. Generates
     *      SELECT a1, a2,..., an, F1(b1) as f1, F2(b2) as f2,...., Fm(bm) as fm, c1,...., (cm), group
     * part in the optimized plan mentioned above
     *
     * It also populates the mappings of new function's output symbol to corresponding old function's output symbol, e.g.
     *     { f1 -> F1, f2 -> F2, ... }
     * The new AggregateNode aggregates on the symbols that original AggregationNode aggregated on
     * Original one will now aggregate on the output symbols of this new node
     */
    private AggregationNode createNonDistinctAggregation(
            AggregationNode node,
            Context context,
            Set<Symbol> groupByKeys,
            GroupIdNode groupIdNode,
            Map<FunctionCall, FunctionCall> nonDistinctAggrFunctionCallMapMapBuilder,
            ImmutableMap.Builder<Symbol, Symbol> aggregationOutputSymbolsMapBuilder)
    {
        SymbolAllocator symbolAllocator = context.getSymbolAllocator();
        ImmutableMap.Builder<Symbol, Aggregation> aggregationsBuilder = ImmutableMap.builder(); // For first aggregation node
        for (Map.Entry<Symbol, Aggregation> entry : node.getAggregations().entrySet()) {
            FunctionCall functionCall = entry.getValue().getCall();
            if (!functionCall.isDistinct()) {
                Symbol newAggregationSymbol = symbolAllocator.newSymbol(entry.getKey().toSymbolReference(), symbolAllocator.getTypes().get(entry.getKey()));
                aggregationOutputSymbolsMapBuilder.put(newAggregationSymbol, entry.getKey());
                aggregationsBuilder.put(newAggregationSymbol, new Aggregation(nonDistinctAggrFunctionCallMapMapBuilder.get(functionCall), entry.getValue().getSignature(), Optional.empty()));
            }
        }

        return new AggregationNode(
                context.getIdAllocator().getNextId(),
                groupIdNode,
                aggregationsBuilder.build(),
                singleGroupingSet(ImmutableList.copyOf(groupByKeys)),
                ImmutableList.of(),
                SINGLE,
                node.getHashSymbol(),
                Optional.empty());
    }
}
