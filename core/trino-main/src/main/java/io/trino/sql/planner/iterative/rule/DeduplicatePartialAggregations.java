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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.trino.matching.Captures;
import io.trino.matching.Pattern;
import io.trino.spi.function.BoundSignature;
import io.trino.spi.function.CatalogSchemaFunctionName;
import io.trino.spi.type.Type;
import io.trino.sql.ir.Expression;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.iterative.Rule;
import io.trino.sql.planner.plan.AggregationNode;
import io.trino.sql.planner.plan.AggregationNode.Aggregation;
import io.trino.sql.planner.plan.Assignments;
import io.trino.sql.planner.plan.ProjectNode;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;

import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.trino.metadata.GlobalFunctionCatalog.builtinFunctionName;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.sql.planner.plan.Patterns.Aggregation.step;
import static io.trino.sql.planner.plan.Patterns.aggregation;
import static java.util.Objects.requireNonNull;

/// Merges aggregations of a PARTIAL or INTERMEDIATE node that compute the same intermediate state,
/// and restores the merged outputs with a projection.
///
/// From:
/// ```
/// - Aggregation (PARTIAL)
///     a := variance(x)
///     b := stddev(x)
/// ```
/// To:
/// ```
/// - Project
///     a := a
///     b := a
///   - Aggregation (PARTIAL)
///       a := variance(x)
/// ```
public class DeduplicatePartialAggregations
        implements Rule<AggregationNode>
{
    private static final Pattern<AggregationNode> PATTERN = aggregation()
            .with(step().matching(AggregationNode.Step::isOutputPartial));

    /// Groups of output functions of one `@AggregationFunction` class, for one input function. They share
    /// the input function, combine function and state, so for equal arguments their intermediate states are equal.
    @VisibleForTesting
    static final Set<Set<FunctionSignature>> SHARED_STATE_FUNCTIONS = ImmutableSet.of(
            sharedState(ImmutableList.of(DOUBLE), "variance", "var_pop", "stddev", "stddev_pop"),
            sharedState(ImmutableList.of(BIGINT), "variance", "var_pop", "stddev", "stddev_pop"),
            sharedState(ImmutableList.of(DOUBLE, DOUBLE), "covar_samp", "covar_pop"),
            sharedState(ImmutableList.of(REAL, REAL), "covar_samp", "covar_pop"),
            sharedState(ImmutableList.of(DOUBLE, DOUBLE), "regr_slope", "regr_intercept"),
            sharedState(ImmutableList.of(REAL, REAL), "regr_slope", "regr_intercept"),
            sharedState(ImmutableList.of(DOUBLE), "skewness", "kurtosis"),
            sharedState(ImmutableList.of(BIGINT), "skewness", "kurtosis"));

    private static final Map<FunctionSignature, Set<FunctionSignature>> SHARED_STATE_GROUPS = SHARED_STATE_FUNCTIONS.stream()
            .flatMap(group -> group.stream().map(function -> Map.entry(function, group)))
            .collect(toImmutableMap(Map.Entry::getKey, Map.Entry::getValue));

    @Override
    public Pattern<AggregationNode> getPattern()
    {
        return PATTERN;
    }

    @Override
    public Result apply(AggregationNode node, Captures captures, Context context)
    {
        Map<SharedState, Symbol> sharedStates = new HashMap<>();
        Map<Symbol, Symbol> merged = new HashMap<>();
        ImmutableMap.Builder<Symbol, Aggregation> aggregations = ImmutableMap.builder();
        for (Map.Entry<Symbol, Aggregation> entry : node.getAggregations().entrySet()) {
            Optional<Symbol> shared = SharedState.of(entry.getValue())
                    .map(state -> sharedStates.putIfAbsent(state, entry.getKey()));
            if (shared.isPresent()) {
                merged.put(entry.getKey(), shared.get());
            }
            else {
                aggregations.put(entry);
            }
        }

        if (merged.isEmpty()) {
            return Result.empty();
        }

        Assignments.Builder assignments = Assignments.builder();
        for (Symbol output : node.getOutputSymbols()) {
            assignments.put(output, merged.getOrDefault(output, output).toSymbolReference());
        }
        AggregationNode deduplicated = AggregationNode.builderFrom(node)
                .setAggregations(aggregations.buildOrThrow())
                .build();
        return Result.ofPlanNode(new ProjectNode(context.getIdAllocator().getNextId(), deduplicated, assignments.build()));
    }

    private static Set<FunctionSignature> sharedState(List<Type> argumentTypes, String... names)
    {
        return Stream.of(names)
                .map(name -> new FunctionSignature(builtinFunctionName(name), argumentTypes))
                .collect(toImmutableSet());
    }

    @VisibleForTesting
    record FunctionSignature(CatalogSchemaFunctionName name, List<Type> argumentTypes)
    {
        FunctionSignature
        {
            requireNonNull(name, "name is null");
            argumentTypes = ImmutableList.copyOf(argumentTypes);
        }
    }

    private record SharedState(
            Set<FunctionSignature> functions,
            List<Expression> arguments,
            boolean distinct,
            Optional<Symbol> filter,
            Optional<Symbol> mask)
    {
        static Optional<SharedState> of(Aggregation aggregation)
        {
            BoundSignature signature = aggregation.getResolvedFunction().signature();
            return Optional.ofNullable(SHARED_STATE_GROUPS.get(new FunctionSignature(signature.getName(), signature.getArgumentTypes())))
                    .map(functions -> new SharedState(
                            functions,
                            aggregation.getArguments(),
                            aggregation.isDistinct(),
                            aggregation.getFilter(),
                            aggregation.getMask()));
        }
    }
}
