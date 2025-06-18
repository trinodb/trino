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
import io.trino.matching.Capture;
import io.trino.matching.Captures;
import io.trino.matching.Pattern;
import io.trino.sql.PlannerContext;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.iterative.Rule;
import io.trino.sql.planner.optimizations.PlanNodeDecorrelator;
import io.trino.sql.planner.plan.AggregationNode;
import io.trino.sql.planner.plan.AssignUniqueId;
import io.trino.sql.planner.plan.Assignments;
import io.trino.sql.planner.plan.CorrelatedJoinNode;
import io.trino.sql.planner.plan.JoinNode;
import io.trino.sql.planner.plan.JoinType;
import io.trino.sql.planner.plan.Patterns;
import io.trino.sql.planner.plan.PlanNode;
import io.trino.sql.planner.plan.ProjectNode;

import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.matching.Capture.newCapture;
import static io.trino.matching.Pattern.nonEmpty;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.sql.ir.Booleans.TRUE;
import static io.trino.sql.planner.plan.AggregationNode.singleGroupingSet;
import static io.trino.sql.planner.plan.JoinType.LEFT;
import static io.trino.sql.planner.plan.Patterns.CorrelatedJoin.filter;
import static io.trino.sql.planner.plan.Patterns.CorrelatedJoin.subquery;
import static io.trino.sql.planner.plan.Patterns.CorrelatedJoin.type;
import static io.trino.sql.planner.plan.Patterns.aggregation;
import static io.trino.sql.planner.plan.Patterns.correlatedJoin;
import static io.trino.sql.planner.plan.Patterns.project;
import static io.trino.sql.planner.plan.Patterns.source;
import static java.util.Objects.requireNonNull;

/**
 * This rule decorrelates a correlated subquery of LEFT correlated join with distinct operator (grouped aggregation with no aggregation assignments)
 * <p>
 * Transforms:
 * <pre>{@code
 * - CorrelatedJoin LEFT (correlation: [c], filter: true, output: a, x)
 *      - Input (a, c)
 *      - Project (x <- b + 100)
 *           - Aggregation "distinct operator" group by [b]
 *                - Source (b) with correlated filter (b > c)
 * }</pre>
 * Into:
 * <pre>{@code
 * - Project (a <- a, x <- b + 100)
 *      - Aggregation "distinct operator" group by [a, c, unique, b]
 *           - LEFT join (filter: b > c)
 *                - UniqueId (unique)
 *                     - Input (a, c)
 *                - Source (b) decorrelated
 * }</pre>
 */
public class TransformCorrelatedDistinctAggregationWithProjection
        implements Rule<CorrelatedJoinNode>
{
    private static final Capture<ProjectNode> PROJECTION = newCapture();
    private static final Capture<AggregationNode> AGGREGATION = newCapture();

    private static final Pattern<CorrelatedJoinNode> PATTERN = correlatedJoin()
            .with(type().equalTo(LEFT))
            .with(nonEmpty(Patterns.CorrelatedJoin.correlation()))
            .with(filter().equalTo(TRUE))
            .with(subquery().matching(project()
                    .capturedAs(PROJECTION)
                    .with(source().matching(aggregation()
                            .matching(AggregationDecorrelation::isDistinctOperator)
                            .capturedAs(AGGREGATION)))));

    private final PlannerContext plannerContext;

    public TransformCorrelatedDistinctAggregationWithProjection(PlannerContext plannerContext)
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
                JoinType.LEFT,
                inputWithUniqueId,
                source,
                ImmutableList.of(),
                inputWithUniqueId.getOutputSymbols(),
                source.getOutputSymbols(),
                false,
                decorrelatedSource.get().getCorrelatedPredicates(),
                Optional.empty(),
                Optional.empty(),
                ImmutableMap.of(),
                Optional.empty());

        // restore aggregation
        AggregationNode aggregation = captures.get(AGGREGATION);
        aggregation = new AggregationNode(
                aggregation.getId(),
                join,
                aggregation.getAggregations(),
                singleGroupingSet(ImmutableList.<Symbol>builder()
                        .addAll(join.getLeftOutputSymbols())
                        .addAll(aggregation.getGroupingKeys())
                        .build()),
                ImmutableList.of(),
                aggregation.getStep(),
                Optional.empty(),
                Optional.empty());

        // restrict outputs and apply projection
        Set<Symbol> outputSymbols = new HashSet<>(correlatedJoinNode.getOutputSymbols());
        List<Symbol> expectedAggregationOutputs = aggregation.getOutputSymbols().stream()
                .filter(outputSymbols::contains)
                .collect(toImmutableList());

        Assignments assignments = Assignments.builder()
                .putIdentities(expectedAggregationOutputs)
                .putAll(captures.get(PROJECTION).getAssignments())
                .build();

        return Result.ofPlanNode(new ProjectNode(
                context.getIdAllocator().getNextId(),
                aggregation,
                assignments));
    }
}
