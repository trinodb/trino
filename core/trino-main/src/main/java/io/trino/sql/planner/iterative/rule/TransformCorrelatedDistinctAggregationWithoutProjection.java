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
import io.trino.matching.Capture;
import io.trino.matching.Captures;
import io.trino.matching.Pattern;
import io.trino.sql.PlannerContext;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.iterative.Rule;
import io.trino.sql.planner.optimizations.PlanNodeDecorrelator;
import io.trino.sql.planner.plan.AggregationNode;
import io.trino.sql.planner.plan.AssignUniqueId;
import io.trino.sql.planner.plan.CorrelatedJoinNode;
import io.trino.sql.planner.plan.JoinNode;
import io.trino.sql.planner.plan.Patterns;
import io.trino.sql.planner.plan.PlanNode;

import java.util.Optional;

import static io.trino.matching.Capture.newCapture;
import static io.trino.matching.Pattern.nonEmpty;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.sql.planner.iterative.rule.Util.restrictOutputs;
import static io.trino.sql.planner.plan.AggregationNode.singleGroupingSet;
import static io.trino.sql.planner.plan.CorrelatedJoinNode.Type.LEFT;
import static io.trino.sql.planner.plan.Patterns.CorrelatedJoin.filter;
import static io.trino.sql.planner.plan.Patterns.CorrelatedJoin.subquery;
import static io.trino.sql.planner.plan.Patterns.CorrelatedJoin.type;
import static io.trino.sql.planner.plan.Patterns.aggregation;
import static io.trino.sql.planner.plan.Patterns.correlatedJoin;
import static io.trino.sql.tree.BooleanLiteral.TRUE_LITERAL;
import static java.util.Objects.requireNonNull;

/**
 * This rule decorrelates a correlated subquery of LEFT correlated join with distinct operator (grouped aggregation with no aggregation assignments)
 * It is similar to TransformCorrelatedDistinctAggregationWithProjection rule, but does not support projection over aggregation in the subquery
 * <p>
 * Transforms:
 * <pre>
 * - CorrelatedJoin LEFT (correlation: [c], filter: true, output: a, b)
 *      - Input (a, c)
 *      - Aggregation "distinct operator" group by [b]
 *           - Source (b) with correlated filter (b > c)
 * </pre>
 * Into:
 * <pre>
 * - Project (a <- a, b <- b)
 *      - Aggregation "distinct operator" group by [a, c, unique, b]
 *           - LEFT join (filter: b > c)
 *                - UniqueId (unique)
 *                     - Input (a, c)
 *                - Source (b) decorrelated
 * </pre>
 */
public class TransformCorrelatedDistinctAggregationWithoutProjection
        implements Rule<CorrelatedJoinNode>
{
    private static final Capture<AggregationNode> AGGREGATION = newCapture();

    private static final Pattern<CorrelatedJoinNode> PATTERN = correlatedJoin()
            .with(type().equalTo(LEFT))
            .with(nonEmpty(Patterns.CorrelatedJoin.correlation()))
            .with(filter().equalTo(TRUE_LITERAL))
            .with(subquery().matching(aggregation()
                    .matching(AggregationDecorrelation::isDistinctOperator)
                    .capturedAs(AGGREGATION)));

    private final PlannerContext plannerContext;

    public TransformCorrelatedDistinctAggregationWithoutProjection(PlannerContext plannerContext)
    {
        this.plannerContext = requireNonNull(plannerContext, "plannerContext is null");
    }

    @Override
    public Pattern<CorrelatedJoinNode> getPattern()
    {
        return PATTERN;
    }

    @Override
    public Result apply(CorrelatedJoinNode correlatedJoinNode, Captures captures, Context context)
    {
        // decorrelate nested plan
        PlanNodeDecorrelator decorrelator = new PlanNodeDecorrelator(plannerContext, context.getSymbolAllocator(), context.getLookup());
        Optional<PlanNodeDecorrelator.DecorrelatedNode> decorrelatedSource = decorrelator.decorrelateFilters(captures.get(AGGREGATION).getSource(), correlatedJoinNode.getCorrelation());
        if (decorrelatedSource.isEmpty()) {
            return Result.empty();
        }

        PlanNode source = decorrelatedSource.get().getNode();

        // assign unique id on correlated join's input. It will be used to distinguish between original input rows after join
        PlanNode inputWithUniqueId = new AssignUniqueId(
                context.getIdAllocator().getNextId(),
                correlatedJoinNode.getInput(),
                context.getSymbolAllocator().newSymbol("unique", BIGINT));

        JoinNode join = new JoinNode(
                context.getIdAllocator().getNextId(),
                JoinNode.Type.LEFT,
                inputWithUniqueId,
                source,
                ImmutableList.of(),
                inputWithUniqueId.getOutputSymbols(),
                source.getOutputSymbols(),
                false,
                decorrelatedSource.get().getCorrelatedPredicates(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                ImmutableMap.of(),
                Optional.empty());

        // restore aggregation
        AggregationNode aggregation = captures.get(AGGREGATION);
        aggregation = AggregationNode.builderFrom(aggregation)
                .setSource(join)
                .setGroupingSets(
                        singleGroupingSet(ImmutableList.<Symbol>builder()
                                .addAll(join.getLeftOutputSymbols())
                                .addAll(aggregation.getGroupingKeys())
                                .build()))
                .setPreGroupedSymbols(
                        ImmutableList.of())
                .setHashSymbol(
                        Optional.empty())
                .setGroupIdSymbol(Optional.empty())
                .build();

        // restrict outputs
        Optional<PlanNode> project = restrictOutputs(context.getIdAllocator(), aggregation, ImmutableSet.copyOf(correlatedJoinNode.getOutputSymbols()));

        return Result.ofPlanNode(project.orElse(aggregation));
    }
}
