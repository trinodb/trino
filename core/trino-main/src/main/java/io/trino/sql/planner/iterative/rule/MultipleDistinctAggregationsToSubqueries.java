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
import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.trino.cost.TaskCountEstimator;
import io.trino.matching.Captures;
import io.trino.matching.Pattern;
import io.trino.metadata.Metadata;
import io.trino.metadata.ResolvedFunction;
import io.trino.sql.ir.Constant;
import io.trino.sql.ir.Expression;
import io.trino.sql.planner.NodeAndMappings;
import io.trino.sql.planner.PlanCopier;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.iterative.Rule;
import io.trino.sql.planner.plan.AggregationNode;
import io.trino.sql.planner.plan.AggregationNode.Aggregation;
import io.trino.sql.planner.plan.Assignments;
import io.trino.sql.planner.plan.PlanNode;
import io.trino.sql.planner.plan.ProjectNode;
import io.trino.sql.planner.plan.UnionNode;

import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.SystemSessionProperties.getCharVarcharCoercion;
import static io.trino.sql.planner.iterative.rule.DistinctAggregationStrategyChooser.createDistinctAggregationStrategyChooser;
import static io.trino.sql.planner.plan.Patterns.aggregation;
import static java.util.Objects.requireNonNull;

/**
 * Transforms plans of the following shape:
 * <pre>
 * - Aggregation
 *        GROUP BY (k)
 *        F1(DISTINCT a0, a1, ...)
 *        F2(DISTINCT b0, b1, ...)
 *        F3(DISTINCT c0, c1, ...)
 *     - X
 * </pre>
 * into
 * <pre>
 * - Aggregation
 *        GROUP BY (k)
 *        any_value(f1), any_value(f2), any_value(f3)
 *     - Union
 *         - Project (k, f1, f2, NULL AS f3)
 *           - Aggregation
 *               GROUP BY (k)
 *               f1 := F1(DISTINCT a0, a1, ...)
 *               f2 := F2(DISTINCT b0, b1, ...)
 *             - X
 *         - Project (k, NULL AS f1, NULL AS f2, f3)
 *           - Aggregation
 *               GROUP BY (k)
 *               f3 := F3(DISTINCT c0, c1, ...)
 *             - X
 * </pre>
 * <p>
 * This improves plan parallelism and allows {@link SingleDistinctAggregationToGroupBy} to optimize the single input distinct aggregation further.
 * The cost is we calculate X and GROUP BY (k) multiple times, so this rule is only beneficial if the calculations are cheap compared to
 * other distinct aggregation strategies.
 */
public class MultipleDistinctAggregationsToSubqueries
        implements Rule<AggregationNode>
{
    private static final Pattern<AggregationNode> PATTERN = aggregation()
            .matching(MultipleDistinctAggregationsToSubqueries::isAggregationCandidateForSplittingToSubqueries);

    // In addition to this check, DistinctAggregationController.isAggregationSourceSupportedForSubqueries, that accesses Metadata,
    // needs also pass, for the plan to be applicable for this rule,
    public static boolean isAggregationCandidateForSplittingToSubqueries(AggregationNode aggregationNode)
    {
        // TODO: we could support non-distinct aggregations if SingleDistinctAggregationToGroupBy supports it
        return SingleDistinctAggregationToGroupBy.allDistinctAggregates(aggregationNode) &&
                OptimizeMixedDistinctAggregations.hasMultipleDistincts(aggregationNode) &&
                // if we have more than one grouping set, we can have duplicated grouping sets and handling this is complex
                aggregationNode.getGroupingSetCount() == 1;
    }

    private final DistinctAggregationStrategyChooser distinctAggregationStrategyChooser;
    private final Metadata metadata;

    public MultipleDistinctAggregationsToSubqueries(TaskCountEstimator taskCountEstimator, Metadata metadata)
    {
        this.distinctAggregationStrategyChooser = createDistinctAggregationStrategyChooser(taskCountEstimator, metadata);
        this.metadata = requireNonNull(metadata, "metadata is null");
    }

    @Override
    public Pattern<AggregationNode> getPattern()
    {
        return PATTERN;
    }

    @Override
    public Result apply(AggregationNode aggregationNode, Captures captures, Context context)
    {
        if (!distinctAggregationStrategyChooser.shouldSplitToSubqueries(aggregationNode, context.getSession(), context.getStatsProvider(), context.getLookup())) {
            return Result.empty();
        }

        // group aggregations by arguments
        Map<Set<Expression>, Map<Symbol, Aggregation>> aggregationsByArguments = new LinkedHashMap<>(aggregationNode.getAggregations().size());
        // sort the aggregation by output symbol to have consistent union layout
        List<Entry<Symbol, Aggregation>> sortedAggregations = aggregationNode.getAggregations().entrySet()
                .stream()
                .sorted(Comparator.comparing(entry -> entry.getKey().name()))
                .collect(toImmutableList());
        for (Entry<Symbol, Aggregation> entry : sortedAggregations) {
            aggregationsByArguments.compute(ImmutableSet.copyOf(entry.getValue().getArguments()), (_, current) -> {
                if (current == null) {
                    current = new LinkedHashMap<>();
                }
                current.put(entry.getKey(), entry.getValue());
                return current;
            });
        }

        List<Symbol> groupingKeys = aggregationNode.getGroupingKeys();
        List<Symbol> aggregationOutputs = aggregationsByArguments.values().stream()
                .flatMap(aggregations -> aggregations.keySet().stream())
                .collect(toImmutableList());
        List<Symbol> unionAggregationOutputs = aggregationOutputs.stream()
                .map(output -> context.getSymbolAllocator().newSymbol(output))
                .collect(toImmutableList());

        // each sub-aggregation contributes every group once, with NULL in place of the outputs of the other sub-aggregations
        ImmutableList.Builder<PlanNode> unionSources = ImmutableList.builder();
        ImmutableListMultimap.Builder<Symbol, Symbol> unionOutputToInputs = ImmutableListMultimap.builder();
        for (Map<Symbol, Aggregation> aggregations : aggregationsByArguments.values()) {
            SubAggregation subAggregation = buildSubAggregation(aggregationNode, aggregations, context);
            Assignments.Builder assignments = Assignments.builder();
            for (Symbol groupingKey : groupingKeys) {
                Symbol input = subAggregation.symbols().get(groupingKey);
                assignments.putIdentity(input);
                unionOutputToInputs.put(groupingKey, input);
            }
            for (int i = 0; i < aggregationOutputs.size(); i++) {
                Symbol output = aggregationOutputs.get(i);
                Symbol input;
                if (aggregations.containsKey(output)) {
                    input = subAggregation.symbols().get(output);
                    assignments.putIdentity(input);
                }
                else {
                    input = context.getSymbolAllocator().newSymbol(output);
                    assignments.put(input, new Constant(output.type(), null));
                }
                unionOutputToInputs.put(unionAggregationOutputs.get(i), input);
            }
            unionSources.add(new ProjectNode(context.getIdAllocator().getNextId(), subAggregation.node(), assignments.build()));
        }
        UnionNode union = new UnionNode(
                context.getIdAllocator().getNextId(),
                unionSources.build(),
                unionOutputToInputs.build(),
                ImmutableList.<Symbol>builder().addAll(groupingKeys).addAll(unionAggregationOutputs).build());

        // any_value skips NULL, so it picks the single value produced for the group by the owning sub-aggregation
        ImmutableMap.Builder<Symbol, Aggregation> mergedAggregations = ImmutableMap.builder();
        for (int i = 0; i < aggregationOutputs.size(); i++) {
            Symbol output = aggregationOutputs.get(i);
            ResolvedFunction anyValue = metadata.resolveBuiltinFunction(getCharVarcharCoercion(context.getSession()), "any_value", ImmutableList.of(output.type()));
            mergedAggregations.put(output, new Aggregation(
                    anyValue,
                    ImmutableList.of(unionAggregationOutputs.get(i).toSymbolReference()),
                    false,
                    Optional.empty(),
                    Optional.empty(),
                    Optional.empty()));
        }
        return Result.ofPlanNode(AggregationNode.builderFrom(aggregationNode)
                .setSource(union)
                .setAggregations(mergedAggregations.buildOrThrow())
                .build());
    }

    private static SubAggregation buildSubAggregation(AggregationNode aggregationNode, Map<Symbol, Aggregation> aggregations, Context context)
    {
        List<Symbol> originalSymbols = ImmutableList.<Symbol>builder()
                .addAll(aggregationNode.getGroupingKeys())
                .addAll(aggregations.keySet())
                .build();
        // copy the plan so that both plan node ids and symbols are not duplicated between sub aggregations
        NodeAndMappings copied = PlanCopier.copyPlan(
                AggregationNode.builderFrom(aggregationNode).setAggregations(aggregations).build(),
                originalSymbols,
                context.getSymbolAllocator(),
                context.getIdAllocator(),
                context.getLookup());
        ImmutableMap.Builder<Symbol, Symbol> symbols = ImmutableMap.builder();
        for (int i = 0; i < originalSymbols.size(); i++) {
            symbols.put(originalSymbols.get(i), copied.getFields().get(i));
        }
        return new SubAggregation((AggregationNode) copied.getNode(), symbols.buildOrThrow());
    }

    // symbols maps each grouping key and aggregation output of the original aggregation to its counterpart in the copied node
    private record SubAggregation(AggregationNode node, Map<Symbol, Symbol> symbols) {}
}
