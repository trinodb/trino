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
import io.trino.spi.function.CatalogSchemaFunctionName;
import io.trino.sql.PlannerContext;
import io.trino.sql.ir.Expression;
import io.trino.sql.ir.Reference;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.iterative.Rule;
import io.trino.sql.planner.optimizations.PlanNodeDecorrelator;
import io.trino.sql.planner.plan.AggregationNode;
import io.trino.sql.planner.plan.AggregationNode.Aggregation;
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
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.Iterables.getOnlyElement;
import static io.trino.matching.Capture.newCapture;
import static io.trino.matching.Pattern.empty;
import static io.trino.matching.Pattern.nonEmpty;
import static io.trino.metadata.GlobalFunctionCatalog.builtinFunctionName;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.sql.ir.Booleans.TRUE;
import static io.trino.sql.ir.IrUtils.and;
import static io.trino.sql.planner.iterative.rule.AggregationDecorrelation.isDistinctOperator;
import static io.trino.sql.planner.iterative.rule.AggregationDecorrelation.restoreDistinctAggregation;
import static io.trino.sql.planner.iterative.rule.AggregationDecorrelation.rewriteWithMasks;
import static io.trino.sql.planner.plan.AggregationNode.singleGroupingSet;
import static io.trino.sql.planner.plan.JoinType.INNER;
import static io.trino.sql.planner.plan.JoinType.LEFT;
import static io.trino.sql.planner.plan.Patterns.Aggregation.groupingColumns;
import static io.trino.sql.planner.plan.Patterns.CorrelatedJoin.filter;
import static io.trino.sql.planner.plan.Patterns.CorrelatedJoin.subquery;
import static io.trino.sql.planner.plan.Patterns.aggregation;
import static io.trino.sql.planner.plan.Patterns.correlatedJoin;
import static io.trino.sql.planner.plan.Patterns.project;
import static io.trino.sql.planner.plan.Patterns.source;
import static java.util.Objects.requireNonNull;

/**
 * This rule decorrelates a correlated subquery of LEFT or INNER correlated join with:
 * - single global aggregation, or
 * - global aggregation over distinct operator (grouped aggregation with no aggregation assignments),
 * in case when the distinct operator cannot be de-correlated by PlanNodeDecorrelator
 * <p>
 * In the case of single aggregation, it transforms:
 * <pre>{@code
 * - CorrelatedJoin LEFT or INNER (correlation: [c], filter: true, output: a, x, y)
 *      - Input (a, c)
 *      - Project (x <- f(count), y <- f'(agg))
 *           - Aggregation global
 *             count <- count(*)
 *             agg <- agg(b)
 *                - Source (b) with correlated filter (b > c)
 * }</pre>
 * Into:
 * <pre>{@code
 * - Project (a <- a, x <- f(count), y <- f'(agg))
 *      - Aggregation (group by [a, c, unique])
 *        count <- count(*) mask(non_null)
 *        agg <- agg(b) mask(non_null)
 *           - LEFT join (filter: b > c)
 *                - UniqueId (unique)
 *                     - Input (a, c)
 *                - Project (non_null <- TRUE)
 *                     - Source (b) decorrelated
 * }</pre>
 * <p>
 * In the case of global aggregation over distinct operator, it transforms:
 * <pre>{@code
 * - CorrelatedJoin LEFT or INNER (correlation: [c], filter: true, output: a, x, y)
 *      - Input (a, c)
 *      - Project (x <- f(count), y <- f'(agg))
 *           - Aggregation global
 *             count <- count(*)
 *             agg <- agg(b)
 *                - Aggregation "distinct operator" group by [b]
 *                     - Source (b) with correlated filter (b > c)
 * }</pre>
 * Into:
 * <pre>{@code
 * - Project (a <- a, x <- f(count), y <- f'(agg))
 *      - Aggregation (group by [a, c, unique])
 *        count <- count(*) mask(non_null)
 *        agg <- agg(b) mask(non_null)
 *           - Aggregation "distinct operator" group by [a, c, unique, non_null, b]
 *                - LEFT join (filter: b > c)
 *                     - UniqueId (unique)
 *                          - Input (a, c)
 *                     - Project (non_null <- TRUE)
 *                          - Source (b) decorrelated
 * }</pre>
 */
public class TransformCorrelatedGlobalAggregationWithProjection
        implements Rule<CorrelatedJoinNode>
{
    private static final CatalogSchemaFunctionName BOOL_OR = builtinFunctionName("bool_or");
    private static final Capture<ProjectNode> PROJECTION = newCapture();
    private static final Capture<AggregationNode> AGGREGATION = newCapture();
    private static final Capture<PlanNode> SOURCE = newCapture();

    private static final Pattern<CorrelatedJoinNode> PATTERN = correlatedJoin()
            .with(nonEmpty(Patterns.CorrelatedJoin.correlation()))
            .with(filter().equalTo(TRUE))
            .with(subquery().matching(project()
                    .capturedAs(PROJECTION)
                    .with(source().matching(aggregation()
                            .with(empty(groupingColumns()))
                            .with(source().capturedAs(SOURCE))
                            .capturedAs(AGGREGATION)))));

    private final PlannerContext plannerContext;

    public TransformCorrelatedGlobalAggregationWithProjection(PlannerContext plannerContext)
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
        checkArgument(correlatedJoinNode.getType() == INNER || correlatedJoinNode.getType() == LEFT, "unexpected correlated join type: %s", correlatedJoinNode.getType());

        // if there is another aggregation below the AggregationNode, handle both
        PlanNode source = captures.get(SOURCE);

        // if we fail to decorrelate the nested plan, and it contains a distinct operator, we can extract and special-handle the distinct operator
        AggregationNode distinct = null;

        // decorrelate nested plan
        PlanNodeDecorrelator decorrelator = new PlanNodeDecorrelator(plannerContext, context.getSymbolAllocator(), context.getLookup());
        Optional<PlanNodeDecorrelator.DecorrelatedNode> decorrelatedSource = decorrelator.decorrelateFilters(source, correlatedJoinNode.getCorrelation());
        if (decorrelatedSource.isEmpty()) {
            // we failed to decorrelate the nested plan, so check if we can extract a distinct operator from the nested plan
            if (isDistinctOperator(source)) {
                distinct = (AggregationNode) source;
                source = distinct.getSource();
                decorrelatedSource = decorrelator.decorrelateFilters(source, correlatedJoinNode.getCorrelation());
            }
            if (decorrelatedSource.isEmpty()) {
                return Result.empty();
            }
        }

        source = decorrelatedSource.get().getNode();
        Optional<Symbol> nonNull = Optional.empty();

        AggregationNode globalAggregation = captures.get(AGGREGATION);
        // Null-insensitive aggregations don't distinguish empty input from null input, so they don't need a special mask true symbol
        // to distinguish between these two cases after decorrelation. For example, count(*) needs a mask symbol because it returns 0
        // for an empty set but 1 for a set with a single null row, but boolean_or returns null for both an empty set and a set with a single null row.
        if (!isNullInsensitiveAggregation(globalAggregation)) {
            nonNull = Optional.of(context.getSymbolAllocator().newSymbol("non_null", BOOLEAN));
            source = new ProjectNode(
                    context.getIdAllocator().getNextId(),
                    source,
                    Assignments.builder()
                            .putIdentities(source.getOutputSymbols())
                            .put(nonNull.get(), TRUE)
                            .build());
        }

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
                Optional.empty(),
                Optional.empty(),
                ImmutableMap.of(),
                Optional.empty());

        PlanNode root = join;

        // restore distinct aggregation
        if (distinct != null) {
            ImmutableList.Builder<Symbol> distinctSymbols = ImmutableList.<Symbol>builder()
                    .addAll(join.getLeftOutputSymbols())
                    .addAll(distinct.getGroupingKeys());
            nonNull.ifPresent(distinctSymbols::add);

            root = restoreDistinctAggregation(
                    distinct,
                    join,
                    distinctSymbols.build());
        }

        Map<Symbol, Aggregation> aggregations = globalAggregation.getAggregations();
        if (nonNull.isPresent()) {
            // prepare mask symbols for aggregations
            // Every original aggregation agg() will be rewritten to agg() mask(non_null). If the aggregation
            // already has a mask, it will be replaced with conjunction of the existing mask and non_null.
            // This is necessary to restore the original aggregation result in case when:
            // - the nested lateral subquery returned empty result for some input row,
            // - aggregation is null-sensitive, which means that its result over a single null row is different
            //   than result for empty input (with global grouping)
            // It applies to the following aggregate functions: count(*), checksum(), array_agg().
            ImmutableMap.Builder<Symbol, Symbol> masksBuilder = ImmutableMap.builder();
            Assignments.Builder assignmentsBuilder = Assignments.builder();
            for (Map.Entry<Symbol, Aggregation> entry : globalAggregation.getAggregations().entrySet()) {
                Aggregation aggregation = entry.getValue();
                if (aggregation.getMask().isPresent()) {
                    Symbol newMask = context.getSymbolAllocator().newSymbol("mask", BOOLEAN);
                    Expression expression = and(aggregation.getMask().get().toSymbolReference(), nonNull.get().toSymbolReference());
                    assignmentsBuilder.put(newMask, expression);
                    masksBuilder.put(entry.getKey(), newMask);
                }
                else {
                    masksBuilder.put(entry.getKey(), nonNull.get());
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
            aggregations = rewriteWithMasks(globalAggregation.getAggregations(), masksBuilder.buildOrThrow());
        }

        // restore global aggregation
        globalAggregation = new AggregationNode(
                globalAggregation.getId(),
                root,
                aggregations,
                singleGroupingSet(ImmutableList.<Symbol>builder()
                        .addAll(join.getLeftOutputSymbols())
                        .addAll(globalAggregation.getGroupingKeys())
                        .build()),
                ImmutableList.of(),
                globalAggregation.getStep(),
                Optional.empty(),
                Optional.empty());

        // restrict outputs and apply projection
        Set<Symbol> outputSymbols = new HashSet<>(correlatedJoinNode.getOutputSymbols());
        List<Symbol> expectedAggregationOutputs = globalAggregation.getOutputSymbols().stream()
                .filter(outputSymbols::contains)
                .collect(toImmutableList());

        Assignments assignments = Assignments.builder()
                .putIdentities(expectedAggregationOutputs)
                .putAll(captures.get(PROJECTION).getAssignments())
                .build();

        return Result.ofPlanNode(new ProjectNode(
                context.getIdAllocator().getNextId(),
                globalAggregation,
                assignments));
    }

    private static boolean isNullInsensitiveAggregation(AggregationNode node)
    {
        if (node.getAggregations().size() != 1) {
            return false;
        }

        Aggregation aggregation = getOnlyElement(node.getAggregations().values());
        if (aggregation.getFilter().isPresent() || aggregation.getMask().isPresent()) {
            return false;
        }

        return aggregation.getResolvedFunction().name().equals(BOOL_OR) && aggregation.getArguments().getFirst() instanceof Reference;
    }
}
