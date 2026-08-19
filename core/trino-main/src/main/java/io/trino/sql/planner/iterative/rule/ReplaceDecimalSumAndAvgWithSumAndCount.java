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
import io.trino.matching.Captures;
import io.trino.matching.Pattern;
import io.trino.metadata.ResolvedFunction;
import io.trino.operator.scalar.DivideRoundToScale;
import io.trino.spi.function.CatalogSchemaFunctionName;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.Type;
import io.trino.sql.PlannerContext;
import io.trino.sql.analyzer.TypeDescriptorProvider;
import io.trino.sql.ir.Call;
import io.trino.sql.ir.Cast;
import io.trino.sql.ir.Expression;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.iterative.Rule;
import io.trino.sql.planner.plan.AggregationNode;
import io.trino.sql.planner.plan.AggregationNode.Aggregation;
import io.trino.sql.planner.plan.Assignments;
import io.trino.sql.planner.plan.ProjectNode;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;

import static com.google.common.collect.Iterables.getOnlyElement;
import static io.trino.SystemSessionProperties.getCharVarcharCoercion;
import static io.trino.metadata.GlobalFunctionCatalog.builtinFunctionName;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.sql.planner.plan.AggregationNode.Step.SINGLE;
import static io.trino.sql.planner.plan.Patterns.Aggregation.step;
import static io.trino.sql.planner.plan.Patterns.aggregation;
import static java.util.Objects.requireNonNull;

/**
 * Rewrites a decimal {@code avg(x)} into a projection over {@code sum(x)} and {@code count(x)} when
 * the aggregation already computes {@code sum(x)}, so the decimal summation is not done twice.
 * <p>
 * Decimal only: for {@code double}/{@code real}/{@code bigint} avg and count cost about the same.
 */
public class ReplaceDecimalSumAndAvgWithSumAndCount
        implements Rule<AggregationNode>
{
    private static final CatalogSchemaFunctionName AVG_NAME = builtinFunctionName("avg");
    private static final CatalogSchemaFunctionName SUM_NAME = builtinFunctionName("sum");
    private static final CatalogSchemaFunctionName COUNT_NAME = builtinFunctionName("count");

    private static final Pattern<AggregationNode> PATTERN = aggregation()
            .with(step().equalTo(SINGLE));

    private final PlannerContext plannerContext;

    public ReplaceDecimalSumAndAvgWithSumAndCount(PlannerContext plannerContext)
    {
        this.plannerContext = requireNonNull(plannerContext, "plannerContext is null");
    }

    @Override
    public Pattern<AggregationNode> getPattern()
    {
        return PATTERN;
    }

    @Override
    public Result apply(AggregationNode node, Captures captures, Context context)
    {
        Map<Symbol, Aggregation> aggregations = node.getAggregations();

        Map<Symbol, Aggregation> newAggregations = new LinkedHashMap<>(aggregations);
        Assignments.Builder projections = Assignments.builder()
                .putIdentities(node.getGroupingKeys());
        boolean rewritten = false;

        for (Map.Entry<Symbol, Aggregation> entry : aggregations.entrySet()) {
            Symbol symbol = entry.getKey();
            if (!isDecimalAvg(entry.getValue())) {
                projections.putIdentity(symbol);
                continue;
            }
            Aggregation avg = entry.getValue();
            Expression argument = getOnlyElement(avg.getArguments());

            Optional<Symbol> sumSymbol = matchingAggregation(aggregations, SUM_NAME, argument, avg.getMask());
            if (sumSymbol.isEmpty()) {
                projections.putIdentity(symbol);
                continue;
            }

            // Reuse an existing count(x) over the same argument and mask or create new one
            Symbol countSymbol = matchingAggregation(aggregations, COUNT_NAME, argument, avg.getMask())
                    .orElseGet(() -> {
                        ResolvedFunction countFunction = plannerContext.getMetadata()
                                .resolveBuiltinFunction(getCharVarcharCoercion(context.getSession()), "count", TypeDescriptorProvider.fromTypes(argument.type()));
                        Symbol newCount = context.getSymbolAllocator().newSymbol("count", BIGINT);
                        newAggregations.put(newCount, new Aggregation(
                                countFunction,
                                ImmutableList.of(argument),
                                false,
                                Optional.empty(),
                                Optional.empty(),
                                avg.getMask()));
                        return newCount;
                    });

            newAggregations.remove(symbol);

            Type sumType = aggregations.get(sumSymbol.get()).getResolvedFunction().signature().getReturnType();
            ResolvedFunction reconstruct = plannerContext.getMetadata()
                    .resolveBuiltinFunction(getCharVarcharCoercion(context.getSession()), DivideRoundToScale.NAME, TypeDescriptorProvider.fromTypes(sumType, BIGINT));
            Expression average = new Call(reconstruct, ImmutableList.of(
                    sumSymbol.get().toSymbolReference(),
                    countSymbol.toSymbolReference()));

            Type avgOutputType = avg.getResolvedFunction().signature().getReturnType();
            projections.put(symbol, castIfNecessary(average, avgOutputType));
            rewritten = true;
        }

        if (!rewritten) {
            return Result.empty();
        }

        AggregationNode newAggregation = AggregationNode.builderFrom(node)
                .setAggregations(newAggregations)
                .build();

        return Result.ofPlanNode(new ProjectNode(
                context.getIdAllocator().getNextId(),
                newAggregation,
                projections.build()));
    }

    private static boolean isDecimalAvg(Aggregation aggregation)
    {
        return aggregation.getResolvedFunction().signature().getName().equals(AVG_NAME)
                && isPlainSingleArgument(aggregation)
                && getOnlyElement(aggregation.getArguments()).type() instanceof DecimalType;
    }

    private static Optional<Symbol> matchingAggregation(Map<Symbol, Aggregation> aggregations, CatalogSchemaFunctionName name, Expression argument, Optional<Symbol> mask)
    {
        for (Map.Entry<Symbol, Aggregation> candidate : aggregations.entrySet()) {
            Aggregation aggregation = candidate.getValue();
            if (aggregation.getResolvedFunction().signature().getName().equals(name)
                    && isPlainSingleArgument(aggregation)
                    && getOnlyElement(aggregation.getArguments()).equals(argument)
                    && aggregation.getMask().equals(mask)) {
                return Optional.of(candidate.getKey());
            }
        }
        return Optional.empty();
    }

    private static boolean isPlainSingleArgument(Aggregation aggregation)
    {
        return !aggregation.isDistinct()
                && aggregation.getFilter().isEmpty()
                && aggregation.getOrderingScheme().isEmpty()
                && aggregation.getArguments().size() == 1;
    }

    private static Expression castIfNecessary(Expression expression, Type targetType)
    {
        if (expression.type().equals(targetType)) {
            return expression;
        }
        return new Cast(expression, targetType);
    }
}
