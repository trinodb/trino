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
import io.trino.sql.planner.plan.AggregationNode.Aggregation;
import io.trino.sql.planner.plan.AssignUniqueId;
import io.trino.sql.planner.plan.Assignments;
import io.trino.sql.planner.plan.CorrelatedJoinNode;
import io.trino.sql.planner.plan.JoinNode;
import io.trino.sql.planner.plan.Patterns;
import io.trino.sql.planner.plan.PlanNode;
import io.trino.sql.planner.plan.ProjectNode;
import io.trino.sql.tree.Expression;

import java.util.Map;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static io.trino.matching.Capture.newCapture;
import static io.trino.matching.Pattern.empty;
import static io.trino.matching.Pattern.nonEmpty;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.sql.ExpressionUtils.and;
import static io.trino.sql.planner.iterative.rule.AggregationDecorrelation.isDistinctOperator;
import static io.trino.sql.planner.iterative.rule.AggregationDecorrelation.rewriteWithMasks;
import static io.trino.sql.planner.iterative.rule.Util.restrictOutputs;
import static io.trino.sql.planner.plan.AggregationNode.singleGroupingSet;
import static io.trino.sql.planner.plan.CorrelatedJoinNode.Type.INNER;
import static io.trino.sql.planner.plan.CorrelatedJoinNode.Type.LEFT;
import static io.trino.sql.planner.plan.Patterns.Aggregation.groupingColumns;
import static io.trino.sql.planner.plan.Patterns.CorrelatedJoin.filter;
import static io.trino.sql.planner.plan.Patterns.CorrelatedJoin.subquery;
import static io.trino.sql.planner.plan.Patterns.aggregation;
import static io.trino.sql.planner.plan.Patterns.correlatedJoin;
import static io.trino.sql.planner.plan.Patterns.source;
import static io.trino.sql.tree.BooleanLiteral.TRUE_LITERAL;
import static java.util.Objects.requireNonNull;

/**
 * This rule decorrelates a correlated subquery with:
 * - single global aggregation, or
 * - global aggregation over distinct operator (grouped aggregation with no aggregation assignments)
 * It is similar to TransformCorrelatedGlobalAggregationWithProjection rule, but does not support projection over aggregation in the subquery
 * <p>
 * In the case of single aggregation, it transforms:
 * <pre>
 * - CorrelatedJoin LEFT or INNER (correlation: [c], filter: true, output: a, count, agg)
 *      - Input (a, c)
 *      - Aggregation global
 *        count <- count(*)
 *        agg <- agg(b)
 *           - Source (b) with correlated filter (b > c)
 * </pre>
 * Into:
 * <pre>
 * - Project (a <- a, count <- count, agg <- agg)
 *      - Aggregation (group by [a, c, unique])
 *        count <- count(*) mask(non_null)
 *        agg <- agg(b) mask(non_null)
 *           - LEFT join (filter: b > c)
 *                - UniqueId (unique)
 *                     - Input (a, c)
 *                - Project (non_null <- TRUE)
 *                     - Source (b) decorrelated
 * </pre>
 * <p>
 * In the case of global aggregation over distinct operator, it transforms:
 * <pre>
 * - CorrelatedJoin LEFT or INNER (correlation: [c], filter: true, output: a, count, agg)
 *      - Input (a, c)
 *      - Aggregation global
 *        count <- count(*)
 *        agg <- agg(b)
 *           - Aggregation "distinct operator" group by [b]
 *                - Source (b) with correlated filter (b > c)
 * </pre>
 * Into:
 * <pre>
 * - Project (a <- a, count <- count, agg <- agg)
 *      - Aggregation (group by [a, c, unique])
 *        count <- count(*) mask(non_null)
 *        agg <- agg(b) mask(non_null)
 *           - Aggregation "distinct operator" group by [a, c, unique, non_null, b]
 *                - LEFT join (filter: b > c)
 *                     - UniqueId (unique)
 *                          - Input (a, c)
 *                     - Project (non_null <- TRUE)
 *                          - Source (b) decorrelated
 * </pre>
 */
public class TransformCorrelatedGlobalAggregationWithoutProjection
        implements Rule<CorrelatedJoinNode>
{
    private static final Capture<AggregationNode> AGGREGATION = newCapture();
    private static final Capture<PlanNode> SOURCE = newCapture();

    private static final Pattern<CorrelatedJoinNode> PATTERN = correlatedJoin()
            .with(nonEmpty(Patterns.CorrelatedJoin.correlation()))
            .with(filter().equalTo(TRUE_LITERAL)) // todo non-trivial join filter: adding filter/project on top of aggregation
            .with(subquery().matching(aggregation()
                    .with(empty(groupingColumns()))
                    .with(source().capturedAs(SOURCE))
                    .capturedAs(AGGREGATION)));

    private final PlannerContext plannerContext;

    public TransformCorrelatedGlobalAggregationWithoutProjection(PlannerContext plannerContext)
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
        checkArgument(correlatedJoinNode.getType() == INNER || correlatedJoinNode.getType() == LEFT, "unexpected correlated join type: " + correlatedJoinNode.getType());

        // if there is another aggregation below the AggregationNode, handle both
        PlanNode source = captures.get(SOURCE);
        AggregationNode distinct = null;
        if (isDistinctOperator(source)) {
            distinct = (AggregationNode) source;
            source = distinct.getSource();
        }

        // decorrelate nested plan
        PlanNodeDecorrelator decorrelator = new PlanNodeDecorrelator(plannerContext, context.getSymbolAllocator(), context.getLookup());
        Optional<PlanNodeDecorrelator.DecorrelatedNode> decorrelatedSource = decorrelator.decorrelateFilters(source, correlatedJoinNode.getCorrelation());
        if (decorrelatedSource.isEmpty()) {
            return Result.empty();
        }

        source = decorrelatedSource.get().getNode();

        // append non-null symbol on nested plan. It will be used to restore semantics of null-sensitive aggregations after LEFT join
        Symbol nonNull = context.getSymbolAllocator().newSymbol("non_null", BOOLEAN);
        source = new ProjectNode(
                context.getIdAllocator().getNextId(),
                source,
                Assignments.builder()
                        .putIdentities(source.getOutputSymbols())
                        .put(nonNull, TRUE_LITERAL)
                        .build());

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

        PlanNode root = join;

        // restore distinct aggregation
        if (distinct != null) {
            root = new AggregationNode(
                    distinct.getId(),
                    join,
                    distinct.getAggregations(),
                    singleGroupingSet(ImmutableList.<Symbol>builder()
                            .addAll(join.getLeftOutputSymbols())
                            .add(nonNull)
                            .addAll(distinct.getGroupingKeys())
                            .build()),
                    ImmutableList.of(),
                    distinct.getStep(),
                    Optional.empty(),
                    Optional.empty());
        }

        // prepare mask symbols for aggregations
        // Every original aggregation agg() will be rewritten to agg() mask(non_null). If the aggregation
        // already has a mask, it will be replaced with conjunction of the existing mask and non_null.
        // This is necessary to restore the original aggregation result in case when:
        // - the nested lateral subquery returned empty result for some input row,
        // - aggregation is null-sensitive, which means that its result over a single null row is different
        //   than result for empty input (with global grouping)
        // It applies to the following aggregate functions: count(*), checksum(), array_agg().
        AggregationNode globalAggregation = captures.get(AGGREGATION);
        ImmutableMap.Builder<Symbol, Symbol> masks = ImmutableMap.builder();
        Assignments.Builder assignmentsBuilder = Assignments.builder();
        for (Map.Entry<Symbol, Aggregation> entry : globalAggregation.getAggregations().entrySet()) {
            Aggregation aggregation = entry.getValue();
            if (aggregation.getMask().isPresent()) {
                Symbol newMask = context.getSymbolAllocator().newSymbol("mask", BOOLEAN);
                Expression expression = and(aggregation.getMask().get().toSymbolReference(), nonNull.toSymbolReference());
                assignmentsBuilder.put(newMask, expression);
                masks.put(entry.getKey(), newMask);
            }
            else {
                masks.put(entry.getKey(), nonNull);
            }
        }
        Assignments maskAssignments = assignmentsBuilder.build();
        if (!maskAssignments.isEmpty()) {
            root = new ProjectNode(
                    context.getIdAllocator().getNextId(),
                    root,
                    Assignments.builder()
                            .putIdentities(root.getOutputSymbols())
                            .putAll(maskAssignments)
                            .build());
        }

        // restore global aggregation
        globalAggregation = new AggregationNode(
                globalAggregation.getId(),
                root,
                rewriteWithMasks(globalAggregation.getAggregations(), masks.buildOrThrow()),
                singleGroupingSet(ImmutableList.<Symbol>builder()
                        .addAll(join.getLeftOutputSymbols())
                        .addAll(globalAggregation.getGroupingKeys())
                        .build()),
                ImmutableList.of(),
                globalAggregation.getStep(),
                Optional.empty(),
                Optional.empty());

        // restrict outputs
        Optional<PlanNode> project = restrictOutputs(context.getIdAllocator(), globalAggregation, ImmutableSet.copyOf(correlatedJoinNode.getOutputSymbols()));

        return Result.ofPlanNode(project.orElse(globalAggregation));
    }
}
