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
import io.trino.cost.TaskCountEstimator;
import io.trino.matching.Captures;
import io.trino.matching.Pattern;
import io.trino.metadata.FunctionResolver;
import io.trino.security.AllowAllAccessControl;
import io.trino.spi.function.CatalogSchemaFunctionName;
import io.trino.sql.PlannerContext;
import io.trino.sql.ir.Coalesce;
import io.trino.sql.ir.Comparison;
import io.trino.sql.ir.Constant;
import io.trino.sql.ir.Expression;
import io.trino.sql.planner.OptimizerConfig.DistinctAggregationsStrategy;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.SymbolAllocator;
import io.trino.sql.planner.iterative.Rule;
import io.trino.sql.planner.plan.AggregationNode;
import io.trino.sql.planner.plan.AggregationNode.Aggregation;
import io.trino.sql.planner.plan.Assignments;
import io.trino.sql.planner.plan.GroupIdNode;
import io.trino.sql.planner.plan.ProjectNode;
import io.trino.sql.tree.QualifiedName;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.trino.SystemSessionProperties.distinctAggregationsStrategy;
import static io.trino.metadata.GlobalFunctionCatalog.builtinFunctionName;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.sql.analyzer.TypeSignatureProvider.fromTypes;
import static io.trino.sql.ir.Comparison.Operator.EQUAL;
import static io.trino.sql.planner.OptimizerConfig.DistinctAggregationsStrategy.AUTOMATIC;
import static io.trino.sql.planner.OptimizerConfig.DistinctAggregationsStrategy.PRE_AGGREGATE;
import static io.trino.sql.planner.iterative.rule.DistinctAggregationStrategyChooser.createDistinctAggregationStrategyChooser;
import static io.trino.sql.planner.plan.AggregationNode.Step.SINGLE;
import static io.trino.sql.planner.plan.AggregationNode.singleGroupingSet;
import static io.trino.sql.planner.plan.Patterns.aggregation;
import static java.util.Map.Entry.comparingByValue;

/*
 * This optimizer rule convert query of form:
 *
 *  SELECT a1, a2,..., an, F1(b1), F2(b2), F3(b3), ...., Fm(bm), F1(distinct c1), ...., Fm(distinct ck) FROM Table GROUP BY a1, a2, ..., an
 *
 *  INTO
 *
 *  SELECT a1, a2,..., an, arbitrary(f1) FILTER (WHERE group=0),...., arbitrary(fm) FILTER (WHERE group=0), F(c1) FILTER (WHERE group=1), ...., F(cm) FILTER (WHERE group=m) FROM
 *      SELECT a1, a2,..., an, F1(b1) as f1, F2(b2) as f2,...., Fm(bm) as fm, c1,..., ck, group FROM
 *        SELECT a1, a2,..., an, b1, b2, ... ,bn, c1,..., ck, group FROM Table
 *        GROUP-ID GROUPING SETS ((a1, a2,..., an, b1, b2, ... ,bn), (a1, a2,..., an, c1), ..., ((a1, a2,..., an, ck)))
 *      GROUP BY a1, a2,..., an, c1,..., ck, group
 *  GROUP BY a1, a2,..., an
 */
public class OptimizeMixedDistinctAggregations
        implements Rule<AggregationNode>
{
    private static final CatalogSchemaFunctionName COUNT_NAME = builtinFunctionName("count");
    private static final CatalogSchemaFunctionName COUNT_IF_NAME = builtinFunctionName("count_if");
    private static final CatalogSchemaFunctionName APPROX_DISTINCT_NAME = builtinFunctionName("approx_distinct");

    private static final Pattern<AggregationNode> PATTERN = aggregation()
            .matching(OptimizeMixedDistinctAggregations::canUsePreAggregate);

    public static boolean canUsePreAggregate(AggregationNode aggregationNode)
    {
        // single distinct can be supported in this rule, but it is already supported by SingleDistinctAggregationToGroupBy, which produces simpler plans (without group-id)
        return (hasMultipleDistincts(aggregationNode) || hasMixedDistinctAndNonDistincts(aggregationNode)) &&
               allDistinctAggregationsHaveSingleArgument(aggregationNode) &&
               noFilters(aggregationNode) &&
               noMasks(aggregationNode) &&
               !aggregationNode.hasOrderings() &&
               aggregationNode.getStep().equals(SINGLE);
    }

    public static boolean hasMultipleDistincts(AggregationNode aggregationNode)
    {
        return distinctAggregationsUniqueArgumentCount(aggregationNode) > 1;
    }

    public static long distinctAggregationsUniqueArgumentCount(AggregationNode aggregationNode)
    {
        return aggregationNode.getAggregations()
                       .values().stream()
                       .filter(Aggregation::isDistinct)
                       .map(Aggregation::getArguments)
                       .map(HashSet::new)
                       .distinct()
                       .count();
    }

    private static boolean hasMixedDistinctAndNonDistincts(AggregationNode aggregationNode)
    {
        long distincts = aggregationNode.getAggregations()
                .values().stream()
                .filter(Aggregation::isDistinct)
                .count();

        return distincts > 0 && distincts < aggregationNode.getAggregations().size();
    }

    private static boolean allDistinctAggregationsHaveSingleArgument(AggregationNode aggregation)
    {
        return aggregation.getAggregations()
                .values().stream()
                .filter(Aggregation::isDistinct)
                .allMatch(node -> node.getArguments().size() == 1);
    }

    private static boolean noFilters(AggregationNode aggregationNode)
    {
        return aggregationNode.getAggregations()
                .values().stream()
                .noneMatch(aggregation -> aggregation.getFilter().isPresent());
    }

    private static boolean noMasks(AggregationNode aggregationNode)
    {
        return aggregationNode.getAggregations()
                .values().stream()
                .noneMatch(aggregation -> aggregation.getMask().isPresent());
    }

    private final FunctionResolver functionResolver;
    private final DistinctAggregationStrategyChooser distinctAggregationStrategyChooser;

    public OptimizeMixedDistinctAggregations(PlannerContext plannerContext, TaskCountEstimator taskCountEstimator)
    {
        this.functionResolver = plannerContext.getFunctionResolver();
        this.distinctAggregationStrategyChooser = createDistinctAggregationStrategyChooser(taskCountEstimator, plannerContext.getMetadata());
    }

    @Override
    public Pattern<AggregationNode> getPattern()
    {
        return PATTERN;
    }

    @Override
    public Result apply(AggregationNode node, Captures captures, Context context)
    {
        DistinctAggregationsStrategy distinctAggregationsStrategy = distinctAggregationsStrategy(context.getSession());

        if (!(distinctAggregationsStrategy.equals(PRE_AGGREGATE) ||
                (distinctAggregationsStrategy.equals(AUTOMATIC) && distinctAggregationStrategyChooser.shouldUsePreAggregate(node, context.getSession(), context.getStatsProvider(), context.getLookup())))) {
            return Result.empty();
        }

        SymbolAllocator symbolAllocator = context.getSymbolAllocator();

        Set<Symbol> originalDistinctAggregationArguments = node.getAggregations().values()
                .stream()
                .filter(Aggregation::isDistinct)
                .flatMap(aggregation -> aggregation.getArguments().stream())
                .map(Symbol::from)
                .collect(toImmutableSet());
        boolean hasNonDistinctAggregation = node.getAggregations().values().stream().anyMatch(aggregation -> !aggregation.isDistinct());

        // each distinct aggregation argument will be part of the GroupId output so first
        // create a mapping between distinct aggregation argument and a id of a group that this symbol is going to be part of
        ImmutableMap.Builder<Symbol, Integer> distinctAggregationArgumentToGroupIdMapBuilder = ImmutableMap.builder();
        // if there are no non-distinct aggregations, we don't add the non-distinct grouping set
        int distinctGroupId = hasNonDistinctAggregation ? 1 : 0;
        for (Symbol distinctAggregationInput : originalDistinctAggregationArguments) {
            distinctAggregationArgumentToGroupIdMapBuilder.put(distinctAggregationInput, distinctGroupId++);
        }
        Map<Symbol, Integer> distinctAggregationArgumentToGroupIdMap = distinctAggregationArgumentToGroupIdMapBuilder.buildOrThrow();

        ImmutableMap.Builder<Symbol, Symbol> groupIdOutputToInputColumnMapping = ImmutableMap.builder();
        for (Symbol distinctArgumentSymbol : originalDistinctAggregationArguments) {
            groupIdOutputToInputColumnMapping.put(distinctArgumentSymbol, distinctArgumentSymbol);
        }
        for (Symbol groupingKey : node.getGroupingKeys()) {
            groupIdOutputToInputColumnMapping.put(groupingKey, groupingKey);
        }

        Symbol groupSymbol = symbolAllocator.newSymbol("group", BIGINT);
        // groupIdFilters are expressions like group == X, that are going to be used in the outer aggregation to aggregate only the specific group
        Assignments.Builder groupIdFilters = Assignments.builder();
        Symbol nonDistinctGroupFilterSymbol = symbolAllocator.newSymbol("non-distinct-gid-filter", BOOLEAN);
        if (hasNonDistinctAggregation) {
            groupIdFilters.put(nonDistinctGroupFilterSymbol, new Comparison(
                    EQUAL,
                    groupSymbol.toSymbolReference(),
                    new Constant(BIGINT, 0L)));
        }

        ImmutableMap.Builder<Symbol, Aggregation> outerAggregations = ImmutableMap.builder();
        // for de-duplication of group id filters
        Map<Integer, Symbol> groupIdFilterSymbolByGroupId = new HashMap<>();
        // prepare outer aggregations and filter expression, symbol for distinct aggregations
        for (Map.Entry<Symbol, Aggregation> entry : node.getAggregations().entrySet()) {
            Aggregation originalAggregation = entry.getValue();
            if (originalAggregation.isDistinct()) {
                // for the outer aggregation node, replace distinct aggregation with non-distinct aggregation with FILTER (WHERE group_id=X)
                Symbol aggregationInput = Symbol.from(originalAggregation.getArguments().getFirst());
                Integer groupId = distinctAggregationArgumentToGroupIdMap.get(aggregationInput);
                Symbol groupIdFilterSymbol = groupIdFilterSymbolByGroupId.computeIfAbsent(groupId, _ -> {
                    Symbol filterSymbol = symbolAllocator.newSymbol("gid-filter-" + groupId, BOOLEAN);
                    groupIdFilters.put(filterSymbol, new Comparison(
                            EQUAL,
                            groupSymbol.toSymbolReference(),
                            new Constant(BIGINT, (long) groupId)));
                    return filterSymbol;
                });

                outerAggregations.put(entry.getKey(), new Aggregation(
                        originalAggregation.getResolvedFunction(),
                        originalAggregation.getArguments(), // GroupIdNode uses distinct aggregation arguments symbols as outputs
                        false,
                        Optional.of(groupIdFilterSymbol),
                        Optional.empty(),
                        Optional.empty()));
            }
        }

        ImmutableMap.Builder<Symbol, Aggregation> innerAggregations = ImmutableMap.builder();
        // Add coalesce projection node to handle count(), count_if(), approx_distinct() functions return a non-null result without any input
        ImmutableMap.Builder<Symbol, Symbol> coalesceSymbolsBuilder = ImmutableMap.builder();
        ImmutableSet.Builder<Symbol> nonDistinctAggregationArguments = ImmutableSet.builder();
        // this is just to avoid duplicating columns in group 0 in case multiple non-distinct and at least one distinct aggregation use the same symbol
        Map<Symbol, Symbol> duplicatedGroupIdInputToOutput = new HashMap<>();
        if (hasNonDistinctAggregation) {
            // prepare inner and outer aggregations for the non-distinct aggregation
            for (Map.Entry<Symbol, Aggregation> entry : node.getAggregations().entrySet()) {
                Aggregation originalAggregation = entry.getValue();
                if (!originalAggregation.isDistinct()) {
                    // non-distinct aggregation, we need both inner aggregation that does the actual aggregation,
                    // and outer aggregation that get the result from inner using arbitrary function
                    Symbol origalAggregationOutputSymbol = entry.getKey();
                    // first, let's create inner aggregation
                    ImmutableList.Builder<Expression> mappedArguments = ImmutableList.builder();
                    for (Expression argument : originalAggregation.getArguments()) {
                        Symbol argumentSymbol = Symbol.from(argument);

                        Symbol finalArgumentSymbol = argumentSymbol;
                        if (originalDistinctAggregationArguments.contains(argumentSymbol)) {
                            // argument symbol is used both in distinct and non-distinct aggregations
                            // we need to duplicate the column in groupId node so that it is used as argument for non-distinct aggregation but not grouped by
                            finalArgumentSymbol = duplicatedGroupIdInputToOutput.computeIfAbsent(
                                    argumentSymbol,
                                    symbol -> symbolAllocator.newSymbol("gid-non-distinct", symbol.type()));
                        }
                        groupIdOutputToInputColumnMapping.put(finalArgumentSymbol, argumentSymbol);

                        mappedArguments.add(finalArgumentSymbol.toSymbolReference());
                        nonDistinctAggregationArguments.add(finalArgumentSymbol);
                    }

                    Symbol innerAggregationOutputSymbol = symbolAllocator.newSymbol("inner", origalAggregationOutputSymbol.type());

                    Aggregation innerAggregation = new Aggregation(
                            originalAggregation.getResolvedFunction(),
                            mappedArguments.build(),
                            false,
                            Optional.empty(),
                            Optional.empty(),
                            Optional.empty());
                    innerAggregations.put(innerAggregationOutputSymbol, innerAggregation);

                    // second, let's create outer aggregation
                    // The inner aggregation node already does non-distinct aggregations, just extract the single value per group
                    Aggregation outerAggregation = new Aggregation(
                            functionResolver.resolveFunction(
                                    context.getSession(),
                                    QualifiedName.of("arbitrary"),
                                    fromTypes(originalAggregation.getResolvedFunction().signature().getReturnType()),
                                    new AllowAllAccessControl()),
                            ImmutableList.of(innerAggregationOutputSymbol.toSymbolReference()),
                            false,
                            Optional.of(nonDistinctGroupFilterSymbol),
                            Optional.empty(),
                            Optional.empty());
                    Symbol outerAggregationOutputSymbol = origalAggregationOutputSymbol;
                    // handle 0 on empty input aggregations
                    CatalogSchemaFunctionName name = originalAggregation.getResolvedFunction().signature().getName();
                    if (name.equals(COUNT_NAME) || name.equals(COUNT_IF_NAME) || name.equals(APPROX_DISTINCT_NAME)) {
                        Symbol coalesceSymbol = symbolAllocator.newSymbol("coalesce_expr", origalAggregationOutputSymbol.type());
                        outerAggregationOutputSymbol = coalesceSymbol;
                        coalesceSymbolsBuilder.put(coalesceSymbol, origalAggregationOutputSymbol);
                    }

                    outerAggregations.put(outerAggregationOutputSymbol, outerAggregation);
                }
            }
        }

        // we now have all the preparation done, let's create a new plan

        // 1. Add GroupIdNode that will duplicate the input for every distinct aggregation input + for special group 0 for non-distinct aggregations
        GroupIdNode groupIdNode = new GroupIdNode(
                context.getIdAllocator().getNextId(),
                node.getSource(),
                createGroups(node.getGroupingKeys(), nonDistinctAggregationArguments.build(), hasNonDistinctAggregation, distinctAggregationArgumentToGroupIdMap),
                groupIdOutputToInputColumnMapping.buildKeepingLast(), // we use buildKeepingLast not, buildOrThrow because aggregation inputs can use symbols from grouping keys
                ImmutableList.of(),
                groupSymbol);

        // 2. Add inner Aggregation node which has aggregations on non-distinct symbols from the original plan. Generates
        //      SELECT a1, a2,..., an, F1(b1) as f1, F2(b2) as f2,...., Fm(bm) as fm, c1,...., (ck), group
        // part in the optimized plan mentioned above.
        //
        // The inner AggregateNode aggregates on the symbols that the original AggregationNode aggregated on.
        // Outer aggregation node will aggregate on the output symbols of this new node
        //
        Set<Symbol> innerAggregationGropingKeys = ImmutableSet.<Symbol>builder()
                .addAll(node.getGroupingKeys())
                .addAll(originalDistinctAggregationArguments)
                .add(groupSymbol).build();
        AggregationNode innerAggregationNode = new AggregationNode(
                context.getIdAllocator().getNextId(),
                groupIdNode,
                innerAggregations.buildOrThrow(),
                singleGroupingSet(ImmutableList.copyOf(innerAggregationGropingKeys)),
                ImmutableList.of(),
                SINGLE,
                Optional.empty());

        // 3. Add a new project node with FILTER expressions
        groupIdFilters.putIdentities(innerAggregationNode.getOutputSymbols());
        ProjectNode groupIdFiltersProjectNode = new ProjectNode(context.getIdAllocator().getNextId(), innerAggregationNode, groupIdFilters.build());

        // 4. Add outer aggregate node to do second aggregation, handles this part of optimized plan mentioned above:
        //          SELECT a1, a2,..., an, arbitrary(if(group = 0, f1)),...., arbitrary(if(group = 0, fm)), F1(if(group = 1, c)),...., Fm(if(group = 1, c))
        AggregationNode outerAggregationNode = new AggregationNode(
                context.getIdAllocator().getNextId(),
                groupIdFiltersProjectNode,
                outerAggregations.buildOrThrow(),
                node.getGroupingSets(),
                ImmutableList.of(),
                node.getStep(),
                node.getGroupIdSymbol());

        Map<Symbol, Symbol> coalesceSymbols = coalesceSymbolsBuilder.buildOrThrow();
        if (coalesceSymbols.isEmpty()) {
            return Result.ofPlanNode(outerAggregationNode);
        }

        Assignments.Builder outputSymbols = Assignments.builder();
        for (Symbol symbol : outerAggregationNode.getOutputSymbols()) {
            if (coalesceSymbols.containsKey(symbol)) {
                Expression expression = new Coalesce(symbol.toSymbolReference(), new Constant(BIGINT, 0L));
                outputSymbols.put(coalesceSymbols.get(symbol), expression);
            }
            else {
                outputSymbols.putIdentity(symbol);
            }
        }

        return Result.ofPlanNode(new ProjectNode(context.getIdAllocator().getNextId(), outerAggregationNode, outputSymbols.build()));
    }

    // non-distinct group = {grouping keys + nonDistinctAggregationArguments}
    // distinct groups = {grouping keys + distinct aggregation argument}
    // symbols present in Group_i will be set, rest will be Null
    private static List<List<Symbol>> createGroups(
            List<Symbol> groupingKeys,
            Set<Symbol> nonDistinctAggregationArguments,
            boolean hasNonDistinctAggregation,
            Map<Symbol, Integer> distinctAggregationArgumentToGroupIdMap)
    {
        ImmutableList.Builder<List<Symbol>> groups = ImmutableList.builder();

        if (hasNonDistinctAggregation) {
            // non-distinct group
            groups.add(ImmutableList.copyOf(ImmutableSet.<Symbol>builder()
                    .addAll(groupingKeys)
                    .addAll(nonDistinctAggregationArguments)
                    .build()));
        }
        // distinct groups
        distinctAggregationArgumentToGroupIdMap.entrySet().stream()
                .sorted(comparingByValue()) // sorting is necessary because group id is based on the order of grouping sets and must match the id in distinctAggregationArgumentToGroupIdMap
                .forEach(entry -> {
                    Symbol distinctAggregationInput = entry.getKey();
                    groups.add(ImmutableList.copyOf(ImmutableSet.<Symbol>builder().addAll(groupingKeys).add(distinctAggregationInput).build()));
                });
        return groups.build();
    }
}
