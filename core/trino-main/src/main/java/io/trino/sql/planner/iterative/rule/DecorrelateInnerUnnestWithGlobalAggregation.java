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
import com.google.common.collect.Streams;
import io.trino.matching.Captures;
import io.trino.matching.Pattern;
import io.trino.sql.planner.PlanNodeIdAllocator;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.SymbolAllocator;
import io.trino.sql.planner.SymbolsExtractor;
import io.trino.sql.planner.iterative.Lookup;
import io.trino.sql.planner.iterative.Rule;
import io.trino.sql.planner.optimizations.PlanNodeSearcher;
import io.trino.sql.planner.plan.AggregationNode;
import io.trino.sql.planner.plan.AssignUniqueId;
import io.trino.sql.planner.plan.Assignments;
import io.trino.sql.planner.plan.CorrelatedJoinNode;
import io.trino.sql.planner.plan.PlanNode;
import io.trino.sql.planner.plan.PlanNodeId;
import io.trino.sql.planner.plan.ProjectNode;
import io.trino.sql.planner.plan.UnnestNode;
import io.trino.sql.planner.plan.UnnestNode.Mapping;
import io.trino.sql.tree.Expression;
import io.trino.sql.tree.IsNotNullPredicate;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.Iterables.getOnlyElement;
import static io.trino.matching.Pattern.nonEmpty;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.sql.ExpressionUtils.and;
import static io.trino.sql.planner.iterative.rule.AggregationDecorrelation.rewriteWithMasks;
import static io.trino.sql.planner.iterative.rule.Util.restrictOutputs;
import static io.trino.sql.planner.optimizations.QueryCardinalityUtil.isScalar;
import static io.trino.sql.planner.plan.AggregationNode.Step.SINGLE;
import static io.trino.sql.planner.plan.AggregationNode.singleGroupingSet;
import static io.trino.sql.planner.plan.JoinNode.Type.INNER;
import static io.trino.sql.planner.plan.JoinNode.Type.LEFT;
import static io.trino.sql.planner.plan.Patterns.CorrelatedJoin.correlation;
import static io.trino.sql.planner.plan.Patterns.CorrelatedJoin.filter;
import static io.trino.sql.planner.plan.Patterns.correlatedJoin;
import static io.trino.sql.tree.BooleanLiteral.TRUE_LITERAL;

/**
 * This rule finds correlated UnnestNode in CorrelatedJoinNode's subquery and folds
 * them into UnnestNode representing LEFT JOIN UNNEST.
 * This rule transforms plans, where:
 * - UnnestNode in subquery is based only on correlation symbols
 * - UnnestNode in subquery is INNER without filter
 * - subquery contains global aggregation over the result of unnest
 * Additionally, other global aggregations, grouped aggregations and projections
 * in subquery are supported.
 * <p>
 * Transforms:
 * <pre>
 * - CorrelatedJoin (LEFT or INNER) on true, correlation(c1, c2)
 *      - Input (a, c1, c2)
 *      - Aggregation
 *           global grouping
 *           agg <- agg1(x)
 *           - Projection
 *                x <- foo(y)
 *                - Aggregation
 *                     group by (g)
 *                     y <- agg2(u)
 *                     - Unnest INNER
 *                          g <- unnest(c1)
 *                          u <- unnest(c2)
 *                          replicate: ()
 * </pre>
 * Into:
 * <pre>
 * - Projection (restrict outputs)
 *      - Aggregation
 *           group by (a, c1, c2, unique)
 *           agg <- agg1(x) mask(mask_symbol)
 *           - Projection
 *                x <- foo(y)
 *                - Aggregation
 *                     group by (g, a, c1, c2, unique, mask_symbol)
 *                     y <- agg2(u)
 *                     - Projection
 *                          mask_symbol <- ordinality IS NOT NULL
 *                          - Unnest LEFT with ordinality
 *                               g <- unnest(c1)
 *                               u <- unnest(c2)
 *                               replicate: (a, c1, c2, unique)
 *                               - AssignUniqueId unique
 *                                    - Input (a, c1, c2)
 * </pre>
 */
public class DecorrelateInnerUnnestWithGlobalAggregation
        implements Rule<CorrelatedJoinNode>
{
    private static final Pattern<CorrelatedJoinNode> PATTERN = correlatedJoin()
            .with(nonEmpty(correlation()))
            .with(filter().equalTo(TRUE_LITERAL))
            .matching(node -> node.getType() == CorrelatedJoinNode.Type.INNER || node.getType() == CorrelatedJoinNode.Type.LEFT);

    @Override
    public Pattern<CorrelatedJoinNode> getPattern()
    {
        return PATTERN;
    }

    @Override
    public Result apply(CorrelatedJoinNode correlatedJoinNode, Captures captures, Context context)
    {
        // find global aggregation in subquery
        List<PlanNode> globalAggregations = PlanNodeSearcher.searchFrom(correlatedJoinNode.getSubquery(), context.getLookup())
                .where(DecorrelateInnerUnnestWithGlobalAggregation::isGlobalAggregation)
                .recurseOnlyWhen(node -> node instanceof ProjectNode || isGlobalAggregation(node))
                .findAll();

        if (globalAggregations.isEmpty()) {
            return Result.empty();
        }

        // if there are multiple global aggregations, the one that is closest to the source is the "reducing" aggregation, because it reduces multiple input rows to single output row
        AggregationNode reducingAggregation = (AggregationNode) globalAggregations.get(globalAggregations.size() - 1);

        // find unnest in subquery
        Optional<UnnestNode> subqueryUnnest = PlanNodeSearcher.searchFrom(reducingAggregation.getSource(), context.getLookup())
                .where(node -> isSupportedUnnest(node, correlatedJoinNode.getCorrelation(), context.getLookup()))
                .recurseOnlyWhen(node -> node instanceof ProjectNode || isGroupedAggregation(node))
                .findFirst();

        if (subqueryUnnest.isEmpty()) {
            return Result.empty();
        }

        UnnestNode unnestNode = subqueryUnnest.get();

        // assign unique id to input rows to restore semantics of aggregations after rewrite
        PlanNode input = new AssignUniqueId(
                context.getIdAllocator().getNextId(),
                correlatedJoinNode.getInput(),
                context.getSymbolAllocator().newSymbol("unique", BIGINT));

        // pre-project unnest symbols if they were pre-projected in subquery
        // The correlated UnnestNode either unnests correlation symbols directly, or unnests symbols produced by a projection that uses only correlation symbols.
        // Here, any underlying projection that was a source of the correlated UnnestNode, is appended as a source of the rewritten UnnestNode.
        // If the projection is not necessary for UnnestNode (i.e. it does not produce any unnest symbols), it should be pruned afterwards.
        PlanNode unnestSource = context.getLookup().resolve(unnestNode.getSource());
        if (unnestSource instanceof ProjectNode) {
            ProjectNode sourceProjection = (ProjectNode) unnestSource;
            input = new ProjectNode(
                    sourceProjection.getId(),
                    input,
                    Assignments.builder()
                            .putIdentities(input.getOutputSymbols())
                            .putAll(sourceProjection.getAssignments())
                            .build());
        }

        // rewrite correlated join to UnnestNode
        Symbol ordinalitySymbol = unnestNode.getOrdinalitySymbol().orElseGet(() -> context.getSymbolAllocator().newSymbol("ordinality", BIGINT));

        UnnestNode rewrittenUnnest = new UnnestNode(
                context.getIdAllocator().getNextId(),
                input,
                input.getOutputSymbols(),
                unnestNode.getMappings(),
                Optional.of(ordinalitySymbol),
                LEFT,
                Optional.empty());

        // append mask symbol based on ordinality to distinguish between the unnested rows and synthetic null rows
        Symbol mask = context.getSymbolAllocator().newSymbol("mask", BOOLEAN);
        ProjectNode sourceWithMask = new ProjectNode(
                context.getIdAllocator().getNextId(),
                rewrittenUnnest,
                Assignments.builder()
                        .putIdentities(rewrittenUnnest.getOutputSymbols())
                        .put(mask, new IsNotNullPredicate(ordinalitySymbol.toSymbolReference()))
                        .build());

        // restore all projections, grouped aggregations and global aggregations from the subquery
        PlanNode result = rewriteNodeSequence(
                context.getLookup().resolve(correlatedJoinNode.getSubquery()),
                input.getOutputSymbols(),
                mask,
                sourceWithMask,
                reducingAggregation.getId(),
                unnestNode.getId(),
                context.getSymbolAllocator(),
                context.getIdAllocator(),
                context.getLookup());

        // restrict outputs
        return Result.ofPlanNode(restrictOutputs(context.getIdAllocator(), result, ImmutableSet.copyOf(correlatedJoinNode.getOutputSymbols())).orElse(result));
    }

    private static boolean isGlobalAggregation(PlanNode node)
    {
        if (!(node instanceof AggregationNode)) {
            return false;
        }

        AggregationNode aggregationNode = (AggregationNode) node;
        return aggregationNode.hasEmptyGroupingSet() &&
                aggregationNode.getGroupingSetCount() == 1 &&
                aggregationNode.getStep() == SINGLE;
    }

    private static boolean isGroupedAggregation(PlanNode node)
    {
        if (!(node instanceof AggregationNode)) {
            return false;
        }

        AggregationNode aggregationNode = (AggregationNode) node;
        return aggregationNode.hasNonEmptyGroupingSet() &&
                aggregationNode.getGroupingSetCount() == 1 &&
                aggregationNode.getStep() == SINGLE;
    }

    /**
     * This rule supports decorrelation of UnnestNode meeting certain conditions:
     * - the UnnestNode should be based on correlation symbols, that is: either unnest correlation symbols directly,
     * or unnest symbols produced by a projection that uses only correlation symbols.
     * - the UnnestNode should not have any replicate symbols,
     * - the UnnestNode should be of type INNER,
     * - the UnnestNode should not have a filter.
     */
    private static boolean isSupportedUnnest(PlanNode node, List<Symbol> correlation, Lookup lookup)
    {
        if (!(node instanceof UnnestNode)) {
            return false;
        }

        UnnestNode unnestNode = (UnnestNode) node;
        List<Symbol> unnestSymbols = unnestNode.getMappings().stream()
                .map(Mapping::getInput)
                .collect(toImmutableList());
        PlanNode unnestSource = lookup.resolve(unnestNode.getSource());
        Set<Symbol> correlationSymbols = ImmutableSet.copyOf(correlation);
        boolean basedOnCorrelation = correlationSymbols.containsAll(unnestSymbols) ||
                unnestSource instanceof ProjectNode && correlationSymbols.containsAll(SymbolsExtractor.extractUnique(((ProjectNode) unnestSource).getAssignments().getExpressions()));

        return isScalar(unnestNode.getSource(), lookup) &&
                unnestNode.getReplicateSymbols().isEmpty() &&
                basedOnCorrelation &&
                unnestNode.getJoinType() == INNER &&
                (unnestNode.getFilter().isEmpty() || unnestNode.getFilter().get().equals(TRUE_LITERAL));
    }

    private static PlanNode rewriteNodeSequence(PlanNode root, List<Symbol> leftOutputs, Symbol mask, PlanNode sequenceSource, PlanNodeId reducingAggregationId, PlanNodeId correlatedUnnestId, SymbolAllocator symbolAllocator, PlanNodeIdAllocator idAllocator, Lookup lookup)
    {
        // bottom of sequence reached -- attach the rewritten source node
        if (root.getId().equals(correlatedUnnestId)) {
            return sequenceSource;
        }

        PlanNode source = rewriteNodeSequence(lookup.resolve(getOnlyElement(root.getSources())), leftOutputs, mask, sequenceSource, reducingAggregationId, correlatedUnnestId, symbolAllocator, idAllocator, lookup);

        if (isGlobalAggregation(root)) {
            AggregationNode aggregationNode = (AggregationNode) root;
            if (aggregationNode.getId().equals(reducingAggregationId)) {
                return withGroupingAndMask(aggregationNode, leftOutputs, mask, source, symbolAllocator, idAllocator);
            }
            return withGrouping(aggregationNode, leftOutputs, source);
        }

        if (isGroupedAggregation(root)) {
            AggregationNode aggregationNode = (AggregationNode) root;
            return withGrouping(
                    aggregationNode,
                    ImmutableList.<Symbol>builder()
                            .addAll(leftOutputs)
                            .add(mask)
                            .build(),
                    source);
        }

        if (root instanceof ProjectNode) {
            ProjectNode projectNode = (ProjectNode) root;
            return new ProjectNode(
                    projectNode.getId(),
                    source,
                    Assignments.builder()
                            .putAll(projectNode.getAssignments())
                            .putIdentities(leftOutputs)
                            .putIdentities(ImmutableSet.copyOf(source.getOutputSymbols()).contains(mask) ? ImmutableList.of(mask) : ImmutableList.of())
                            .build());
        }

        throw new IllegalStateException("unexpected node: " + root);
    }

    private static AggregationNode withGroupingAndMask(AggregationNode aggregationNode, List<Symbol> groupingSymbols, Symbol mask, PlanNode source, SymbolAllocator symbolAllocator, PlanNodeIdAllocator idAllocator)
    {
        // Every original aggregation agg() will be rewritten to agg() mask(mask_symbol). If the aggregation
        // already has a mask, it will be replaced with conjunction of the existing mask and mask_symbol.
        ImmutableMap.Builder<Symbol, Symbol> masks = ImmutableMap.builder();
        Assignments.Builder assignmentsBuilder = Assignments.builder();
        for (Map.Entry<Symbol, AggregationNode.Aggregation> entry : aggregationNode.getAggregations().entrySet()) {
            AggregationNode.Aggregation aggregation = entry.getValue();
            if (aggregation.getMask().isPresent()) {
                Symbol newMask = symbolAllocator.newSymbol("mask", BOOLEAN);
                Expression expression = and(aggregation.getMask().get().toSymbolReference(), mask.toSymbolReference());
                assignmentsBuilder.put(newMask, expression);
                masks.put(entry.getKey(), newMask);
            }
            else {
                masks.put(entry.getKey(), mask);
            }
        }
        Assignments maskAssignments = assignmentsBuilder.build();
        if (!maskAssignments.isEmpty()) {
            source = new ProjectNode(
                    idAllocator.getNextId(),
                    source,
                    Assignments.builder()
                            .putIdentities(source.getOutputSymbols())
                            .putAll(maskAssignments)
                            .build());
        }

        return new AggregationNode(
                aggregationNode.getId(),
                source,
                rewriteWithMasks(aggregationNode.getAggregations(), masks.buildOrThrow()),
                singleGroupingSet(groupingSymbols),
                ImmutableList.of(),
                SINGLE,
                Optional.empty(),
                Optional.empty());
    }

    private static AggregationNode withGrouping(AggregationNode aggregationNode, List<Symbol> groupingSymbols, PlanNode source)
    {
        AggregationNode.GroupingSetDescriptor groupingSet = singleGroupingSet(Streams.concat(groupingSymbols.stream(), aggregationNode.getGroupingKeys().stream())
                .distinct()
                .collect(toImmutableList()));

        return new AggregationNode(
                aggregationNode.getId(),
                source,
                aggregationNode.getAggregations(),
                groupingSet,
                ImmutableList.of(),
                SINGLE,
                Optional.empty(),
                Optional.empty());
    }
}
