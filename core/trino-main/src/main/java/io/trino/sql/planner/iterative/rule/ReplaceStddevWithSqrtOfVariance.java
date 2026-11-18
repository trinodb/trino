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
import io.trino.matching.Captures;
import io.trino.matching.Pattern;
import io.trino.metadata.Metadata;
import io.trino.metadata.ResolvedFunction;
import io.trino.spi.function.BoundSignature;
import io.trino.spi.function.CatalogSchemaFunctionName;
import io.trino.spi.type.Type;
import io.trino.sql.ir.Call;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.iterative.Rule;
import io.trino.sql.planner.plan.AggregationNode;
import io.trino.sql.planner.plan.AggregationNode.Aggregation;
import io.trino.sql.planner.plan.Assignments;
import io.trino.sql.planner.plan.ProjectNode;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.trino.SystemSessionProperties.getCharVarcharCoercion;
import static io.trino.metadata.GlobalFunctionCatalog.builtinFunctionName;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.sql.planner.plan.AggregationNode.Step.SINGLE;
import static io.trino.sql.planner.plan.Patterns.aggregation;
import static java.util.Objects.requireNonNull;

/**
 * Computes {@code stddev(x)} as {@code sqrt(variance(x))} when {@code variance(x)}
 * is computed by the same aggregation, and likewise {@code stddev_pop(x)} as
 * {@code sqrt(var_pop(x))}, so that the shared state is aggregated once.
 */
public class ReplaceStddevWithSqrtOfVariance
        implements Rule<AggregationNode>
{
    // stddev and variance are the canonical names of stddev_samp and var_samp
    private static final Map<CatalogSchemaFunctionName, String> STDDEV_TO_VARIANCE = ImmutableMap.of(
            builtinFunctionName("stddev"), "variance",
            builtinFunctionName("stddev_pop"), "var_pop");

    private static final Set<List<Type>> SUPPORTED_ARGUMENT_TYPES = ImmutableSet.of(
            ImmutableList.of(DOUBLE),
            ImmutableList.of(BIGINT));

    private static final Pattern<AggregationNode> PATTERN = aggregation()
            .matching(node -> node.getStep() == SINGLE);

    private final Metadata metadata;

    public ReplaceStddevWithSqrtOfVariance(Metadata metadata)
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
        Set<String> functionNames = node.getAggregations().values().stream()
                .map(aggregation -> aggregation.getResolvedFunction().signature().getName().functionName())
                .collect(toImmutableSet());
        if (STDDEV_TO_VARIANCE.entrySet().stream()
                .noneMatch(entry -> functionNames.contains(entry.getKey().functionName()) && functionNames.contains(entry.getValue()))) {
            return Result.empty();
        }

        Map<Aggregation, Symbol> aggregationOutputs = new HashMap<>();
        node.getAggregations().forEach((symbol, aggregation) -> aggregationOutputs.putIfAbsent(aggregation, symbol));

        Map<Symbol, Aggregation> aggregations = new LinkedHashMap<>(node.getAggregations());
        Map<Symbol, Symbol> derivedStddevs = new HashMap<>();
        for (Map.Entry<Symbol, Aggregation> entry : node.getAggregations().entrySet()) {
            Aggregation aggregation = entry.getValue();
            BoundSignature stddevSignature = aggregation.getResolvedFunction().signature();
            String varianceName = STDDEV_TO_VARIANCE.get(stddevSignature.getName());
            if (varianceName == null || !SUPPORTED_ARGUMENT_TYPES.contains(stddevSignature.getArgumentTypes())) {
                continue;
            }
            ResolvedFunction varianceFunction = metadata.resolveBuiltinFunction(
                    getCharVarcharCoercion(context.getSession()),
                    varianceName,
                    stddevSignature.getArgumentTypes());
            verify(stddevSignature.getReturnType().equals(DOUBLE), "Unexpected return type: %s", stddevSignature);
            verify(varianceFunction.signature().getReturnType().equals(DOUBLE), "Unexpected return type: %s", varianceFunction.signature());
            Aggregation variance = new Aggregation(
                    varianceFunction,
                    aggregation.getArguments(),
                    aggregation.isDistinct(),
                    aggregation.getFilter(),
                    aggregation.getOrderingScheme(),
                    aggregation.getMask());
            Symbol varianceSymbol = aggregationOutputs.get(variance);
            if (varianceSymbol != null) {
                aggregations.remove(entry.getKey());
                derivedStddevs.put(entry.getKey(), varianceSymbol);
            }
        }

        if (derivedStddevs.isEmpty()) {
            return Result.empty();
        }

        ResolvedFunction sqrt = metadata.resolveBuiltinFunction(getCharVarcharCoercion(context.getSession()), "sqrt", ImmutableList.of(DOUBLE));
        Assignments.Builder assignments = Assignments.builder();
        for (Symbol output : node.getOutputSymbols()) {
            Symbol varianceSymbol = derivedStddevs.get(output);
            if (varianceSymbol == null) {
                assignments.putIdentity(output);
            }
            else {
                assignments.put(output, new Call(sqrt, ImmutableList.of(varianceSymbol.toSymbolReference())));
            }
        }

        AggregationNode aggregationNode = AggregationNode.builderFrom(node)
                .setAggregations(aggregations)
                .build();
        return Result.ofPlanNode(new ProjectNode(context.getIdAllocator().getNextId(), aggregationNode, assignments.build()));
    }
}
