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
import io.trino.matching.Captures;
import io.trino.matching.Pattern;
import io.trino.metadata.ResolvedFunction;
import io.trino.spi.type.TypeSignature;
import io.trino.sql.PlannerContext;
import io.trino.sql.planner.FunctionCallBuilder;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.iterative.Rule;
import io.trino.sql.planner.plan.AggregationNode;
import io.trino.sql.planner.plan.AggregationNode.Aggregation;
import io.trino.sql.planner.plan.Assignments;
import io.trino.sql.planner.plan.ProjectNode;
import io.trino.sql.tree.Expression;
import io.trino.sql.tree.FunctionCall;
import io.trino.sql.tree.LongLiteral;
import io.trino.sql.tree.QualifiedName;

import java.util.Map;
import java.util.Optional;

import static com.google.common.collect.Iterables.getOnlyElement;
import static io.trino.SystemSessionProperties.getHashPartitionCount;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.sql.analyzer.TypeSignatureProvider.fromTypeSignatures;
import static io.trino.sql.planner.plan.Patterns.aggregation;
import static java.util.Objects.requireNonNull;

/**
 * Re-writes spatial_partitioning(geometry) aggregations into spatial_partitioning(envelope, partition_count)
 * on top of ST_Envelope(geometry) projection, e.g.
 * <pre>
 * - Aggregation: spatial_partitioning(geometry)
 *    - source
 * </pre>
 * becomes
 * <pre>
 * - Aggregation: spatial_partitioning(envelope, partition_count)
 *    - Project: envelope := ST_Envelope(geometry)
 *        - source
 * </pre>
 * , where partition_count is the value of session property hash_partition_count
 */
public class RewriteSpatialPartitioningAggregation
        implements Rule<AggregationNode>
{
    private static final TypeSignature GEOMETRY_TYPE_SIGNATURE = new TypeSignature("Geometry");
    private static final String NAME = "spatial_partitioning";
    private static final Pattern<AggregationNode> PATTERN = aggregation()
            .matching(RewriteSpatialPartitioningAggregation::hasSpatialPartitioningAggregation);

    private final PlannerContext plannerContext;

    public RewriteSpatialPartitioningAggregation(PlannerContext plannerContext)
    {
        this.plannerContext = requireNonNull(plannerContext, "plannerContext is null");
    }

    private static boolean hasSpatialPartitioningAggregation(AggregationNode aggregationNode)
    {
        return aggregationNode.getAggregations().values().stream()
                .anyMatch(aggregation -> aggregation.getResolvedFunction().getSignature().getName().equals(NAME) && aggregation.getArguments().size() == 1);
    }

    @Override
    public Pattern<AggregationNode> getPattern()
    {
        return PATTERN;
    }

    @Override
    public Result apply(AggregationNode node, Captures captures, Context context)
    {
        ResolvedFunction spatialPartitioningFunction = plannerContext.getMetadata().resolveFunction(context.getSession(), QualifiedName.of(NAME), fromTypeSignatures(GEOMETRY_TYPE_SIGNATURE, INTEGER.getTypeSignature()));
        ResolvedFunction stEnvelopeFunction = plannerContext.getMetadata().resolveFunction(context.getSession(), QualifiedName.of("ST_Envelope"), fromTypeSignatures(GEOMETRY_TYPE_SIGNATURE));

        ImmutableMap.Builder<Symbol, Aggregation> aggregations = ImmutableMap.builder();
        Symbol partitionCountSymbol = context.getSymbolAllocator().newSymbol("partition_count", INTEGER);
        ImmutableMap.Builder<Symbol, Expression> envelopeAssignments = ImmutableMap.builder();
        for (Map.Entry<Symbol, Aggregation> entry : node.getAggregations().entrySet()) {
            Aggregation aggregation = entry.getValue();
            String name = aggregation.getResolvedFunction().getSignature().getName();
            if (name.equals(NAME) && aggregation.getArguments().size() == 1) {
                Expression geometry = getOnlyElement(aggregation.getArguments());
                Symbol envelopeSymbol = context.getSymbolAllocator().newSymbol("envelope", plannerContext.getTypeManager().getType(GEOMETRY_TYPE_SIGNATURE));
                if (isStEnvelopeFunctionCall(geometry, stEnvelopeFunction)) {
                    envelopeAssignments.put(envelopeSymbol, geometry);
                }
                else {
                    envelopeAssignments.put(envelopeSymbol, FunctionCallBuilder.resolve(context.getSession(), plannerContext.getMetadata())
                            .setName(QualifiedName.of("ST_Envelope"))
                            .addArgument(GEOMETRY_TYPE_SIGNATURE, geometry)
                            .build());
                }
                aggregations.put(entry.getKey(),
                        new Aggregation(
                                spatialPartitioningFunction,
                                ImmutableList.of(envelopeSymbol.toSymbolReference(), partitionCountSymbol.toSymbolReference()),
                                false,
                                Optional.empty(),
                                Optional.empty(),
                                aggregation.getMask()));
            }
            else {
                aggregations.put(entry);
            }
        }

        return Result.ofPlanNode(
                new AggregationNode(
                        node.getId(),
                        new ProjectNode(
                                context.getIdAllocator().getNextId(),
                                node.getSource(),
                                Assignments.builder()
                                        .putIdentities(node.getSource().getOutputSymbols())
                                        .put(partitionCountSymbol, new LongLiteral(Integer.toString(getHashPartitionCount(context.getSession()))))
                                        .putAll(envelopeAssignments.buildOrThrow())
                                        .build()),
                        aggregations.buildOrThrow(),
                        node.getGroupingSets(),
                        node.getPreGroupedSymbols(),
                        node.getStep(),
                        node.getHashSymbol(),
                        node.getGroupIdSymbol()));
    }

    private boolean isStEnvelopeFunctionCall(Expression expression, ResolvedFunction stEnvelopeFunction)
    {
        if (!(expression instanceof FunctionCall)) {
            return false;
        }

        FunctionCall functionCall = (FunctionCall) expression;
        return plannerContext.getMetadata().decodeFunction(functionCall.getName())
                .getFunctionId()
                .equals(stEnvelopeFunction.getFunctionId());
    }
}
