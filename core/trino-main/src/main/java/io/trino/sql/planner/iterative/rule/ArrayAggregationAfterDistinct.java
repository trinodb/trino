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

import com.google.common.collect.ImmutableMap;
import io.trino.matching.Capture;
import io.trino.matching.Captures;
import io.trino.matching.Pattern;
import io.trino.metadata.Metadata;
import io.trino.metadata.ResolvedFunction;
import io.trino.operator.aggregation.arrayagg.ArrayAggregationFunction;
import io.trino.operator.scalar.ArrayDistinctFunction;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.SymbolsExtractor;
import io.trino.sql.planner.iterative.Rule;
import io.trino.sql.planner.plan.AggregationNode;
import io.trino.sql.planner.plan.AggregationNode.Aggregation;
import io.trino.sql.planner.plan.Assignments;
import io.trino.sql.planner.plan.ProjectNode;
import io.trino.sql.tree.Expression;
import io.trino.sql.tree.FunctionCall;
import io.trino.sql.tree.SymbolReference;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static com.google.common.collect.Iterables.getOnlyElement;
import static io.trino.matching.Capture.newCapture;
import static io.trino.sql.planner.plan.Patterns.aggregation;
import static io.trino.sql.planner.plan.Patterns.project;
import static io.trino.sql.planner.plan.Patterns.source;
import static java.util.Objects.requireNonNull;

public class ArrayAggregationAfterDistinct
        implements Rule<ProjectNode>
{
    private static final Capture<AggregationNode> CHILD = newCapture();
    private final Metadata metadata;

    public ArrayAggregationAfterDistinct(Metadata metadata)
    {
        this.metadata = requireNonNull(metadata, "metadata is null");
    }

    @Override
    public Pattern<ProjectNode> getPattern()
    {
        return project()
                .matching(entry -> hasArrayDistinct(metadata, entry))
                .with(source().matching(aggregation()
                        .matching(ArrayAggregationAfterDistinct::hasArrayAggregation)
                        .capturedAs(CHILD)));
    }

    private static boolean hasArrayAggregation(AggregationNode aggregationNode)
    {
        return aggregationNode.getAggregations().values().stream()
                .anyMatch(aggregation -> aggregation.getResolvedFunction().getSignature().getName().equals(ArrayAggregationFunction.NAME));
    }

    private static boolean hasArrayDistinct(Metadata metadata, ProjectNode projectNode)
    {
        return projectNode.getAssignments().getExpressions().stream()
                .anyMatch(assignment -> assignment instanceof FunctionCall &&
                        metadata.decodeFunction(((FunctionCall) assignment).getName()).getSignature().getName().equals(ArrayDistinctFunction.NAME));
    }

    @Override
    public Result apply(ProjectNode projectNode, Captures captures, Context context)
    {
        AggregationNode aggregationNode = captures.get(CHILD);

        Assignments.Builder assignmentsBuilder = Assignments.builder();
        ImmutableMap.Builder<Symbol, Aggregation> aggregationsBuilder = ImmutableMap.builder();

        List<Symbol> matchedAggregations = new ArrayList<>();
        List<Symbol> matchedAssignments = new ArrayList<>();

        // Find all array_agg() aggregations
        Set<Symbol> arrayAggSymbols = aggregationNode.getAggregations().entrySet().stream()
                .filter(entry -> entry.getValue().getResolvedFunction().getSignature().getName().equals(ArrayAggregationFunction.NAME))
                .map(Map.Entry::getKey)
                .collect(toImmutableSet());

        // Extract project assignments which are array_distinct(array_agg())
        Map<Symbol, Expression> arrayDistinctCalls = projectNode.getAssignments().entrySet().stream()
                .filter(entry -> entry.getValue() instanceof FunctionCall)
                .filter(entry -> {
                    FunctionCall functionCall = (FunctionCall) entry.getValue();
                    return functionCall.getArguments().size() == 1 &&
                            getOnlyElement(functionCall.getArguments()) instanceof SymbolReference &&
                            arrayAggSymbols.contains(Symbol.from(getOnlyElement(functionCall.getArguments())));
                })
                // We only decode function calls that have an array aggregation as only parameter (because decoding is expensive)
                .filter(entry -> {
                    FunctionCall functionCall = (FunctionCall) entry.getValue();
                    ResolvedFunction resolvedFunction = metadata.decodeFunction(functionCall.getName());
                    return resolvedFunction.getSignature().getName().equals(ArrayDistinctFunction.NAME);
                })
                .collect(toImmutableMap(Map.Entry::getKey, Map.Entry::getValue));

        // Identify array_agg() symbols used outside of array_distinct() calls. Those aggregations won't be rewritten
        Set<Symbol> referencedOutsideArrayDistinctAggregateSymbols = projectNode.getAssignments().entrySet().stream()
                .filter(entry -> !arrayDistinctCalls.containsKey(entry.getKey()))
                .flatMap(entry -> SymbolsExtractor.extractUnique(entry.getValue()).stream())
                .filter(arrayAggSymbols::contains)
                .collect(toImmutableSet());

        // All array_agg aggregations that are only referenced by array_distinct() shall be rewritten
        Map<Symbol, Aggregation> arrayAggregationsToMakeDistinct = arrayDistinctCalls.values().stream()
                .map(expression -> Symbol.from(getOnlyElement(((FunctionCall) expression).getArguments())))
                .filter(symbol -> !referencedOutsideArrayDistinctAggregateSymbols.contains(symbol))
                .distinct()
                .collect(toImmutableMap(symbol -> symbol, symbol -> aggregationNode.getAggregations().get(symbol)));

        // Make the aggregations distinct
        arrayAggregationsToMakeDistinct.forEach((symbol, aggregation) -> {
            if (!aggregation.isDistinct()) {
                matchedAggregations.add(symbol);
                Aggregation newAggregation = new Aggregation(
                        aggregation.getResolvedFunction(),
                        aggregation.getArguments(),
                        true,
                        aggregation.getFilter(),
                        aggregation.getOrderingScheme(),
                        aggregation.getMask());
                aggregationsBuilder.put(symbol, newAggregation);
            }
        });

        // Swap the relevant array_distinct() symbols with the now distinct array_agg() symbols
        arrayDistinctCalls.forEach((assignmentSymbol, arrayDistinctCall) -> {
            FunctionCall functionCall = (FunctionCall) arrayDistinctCall;
            Expression functionArgument = getOnlyElement(functionCall.getArguments());
            Symbol arrayAggSymbol = Symbol.from(functionArgument);
            if (arrayAggregationsToMakeDistinct.containsKey(arrayAggSymbol)) {
                matchedAssignments.add(assignmentSymbol);
                assignmentsBuilder.put(assignmentSymbol, arrayAggSymbol.toSymbolReference());
            }
        });

        if (!matchedAssignments.isEmpty()) {
            // re-add not matched assignments
            projectNode.getAssignments()
                    .filter(entry -> !matchedAssignments.contains(entry))
                    .forEach(assignmentsBuilder::put);

            // re-add not matched aggregations
            aggregationNode.getAggregations().entrySet().stream()
                    .filter(entry -> !matchedAggregations.contains(entry.getKey()))
                    .forEach(entry -> aggregationsBuilder.put(entry.getKey(), entry.getValue()));

            AggregationNode newAggregationNode = new AggregationNode(
                    aggregationNode.getId(),
                    aggregationNode.getSource(),
                    aggregationsBuilder.buildOrThrow(),
                    aggregationNode.getGroupingSets(),
                    aggregationNode.getPreGroupedSymbols(),
                    aggregationNode.getStep(),
                    aggregationNode.getHashSymbol(),
                    aggregationNode.getGroupIdSymbol());
            return Result.ofPlanNode(new ProjectNode(projectNode.getId(), newAggregationNode, assignmentsBuilder.build()));
        }

        return Result.empty();
    }
}
