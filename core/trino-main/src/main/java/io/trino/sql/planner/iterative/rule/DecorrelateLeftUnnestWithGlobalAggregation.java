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

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Streams;
import io.trino.matching.Captures;
import io.trino.matching.Pattern;
import io.trino.sql.planner.Symbol;
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

import java.util.List;
import java.util.Optional;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.Iterables.getOnlyElement;
import static io.trino.matching.Pattern.nonEmpty;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.sql.planner.iterative.rule.Util.restrictOutputs;
import static io.trino.sql.planner.optimizations.QueryCardinalityUtil.isScalar;
import static io.trino.sql.planner.plan.AggregationNode.Step.SINGLE;
import static io.trino.sql.planner.plan.AggregationNode.singleAggregation;
import static io.trino.sql.planner.plan.AggregationNode.singleGroupingSet;
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
 * - UnnestNode in subquery is LEFT without filter
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
 *                     - Unnest LEFT
 *                          g <- unnest(c1)
 *                          u <- unnest(c2)
 *                          replicate: ()
 * </pre>
 * Into:
 * <pre>
 * - Projection (restrict outputs)
 *      - Aggregation
 *           group by (a, c1, c2, unique)
 *           agg <- agg1(x)
 *           - Projection
 *                x <- foo(y)
 *                - Aggregation
 *                     group by (g, a, c1, c2, unique)
 *                     y <- agg2(u)
 *                     - Unnest LEFT
 *                          g <- unnest(c1)
 *                          u <- unnest(c2)
 *                          replicate: (a, c1, c2, unique)
 *                          - AssignUniqueId unique
 *                               - Input (a, c1, c2)
 * </pre>
 */
public class DecorrelateLeftUnnestWithGlobalAggregation
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
        Optional<AggregationNode> globalAggregation = PlanNodeSearcher.searchFrom(correlatedJoinNode.getSubquery(), context.getLookup())
                .where(DecorrelateLeftUnnestWithGlobalAggregation::isGlobalAggregation)
                .recurseOnlyWhen(node -> node instanceof ProjectNode || isGroupedAggregation(node))
                .findFirst();

        if (globalAggregation.isEmpty()) {
            return Result.empty();
        }

        // find unnest in subquery
        Optional<UnnestNode> subqueryUnnest = PlanNodeSearcher.searchFrom(correlatedJoinNode.getSubquery(), context.getLookup())
                .where(node -> isSupportedUnnest(node, correlatedJoinNode.getCorrelation(), context.getLookup()))
                .recurseOnlyWhen(node -> node instanceof ProjectNode || isGlobalAggregation(node) || isGroupedAggregation(node))
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
        if (unnestSource instanceof ProjectNode sourceProjection) {
            input = new ProjectNode(
                    sourceProjection.getId(),
                    input,
                    Assignments.builder()
                            .putIdentities(input.getOutputSymbols())
                            .putAll(sourceProjection.getAssignments())
                            .build());
        }

        // rewrite correlated join to UnnestNode
        UnnestNode rewrittenUnnest = new UnnestNode(
                context.getIdAllocator().getNextId(),
                input,
                input.getOutputSymbols(),
                unnestNode.getMappings(),
                unnestNode.getOrdinalitySymbol(),
                LEFT,
                Optional.empty());

        // restore all projections, grouped aggregations and global aggregations from the subquery
        PlanNode result = rewriteNodeSequence(
                context.getLookup().resolve(correlatedJoinNode.getSubquery()),
                input.getOutputSymbols(),
                rewrittenUnnest,
                unnestNode.getId(),
                context.getLookup());

        // restrict outputs
        return Result.ofPlanNode(restrictOutputs(context.getIdAllocator(), result, ImmutableSet.copyOf(correlatedJoinNode.getOutputSymbols())).orElse(result));
    }

    private static boolean isGlobalAggregation(PlanNode node)
    {
        if (!(node instanceof AggregationNode aggregationNode)) {
            return false;
        }

        return aggregationNode.hasSingleGlobalAggregation() &&
                aggregationNode.getStep() == SINGLE;
    }

    private static boolean isGroupedAggregation(PlanNode node)
    {
        if (!(node instanceof AggregationNode aggregationNode)) {
            return false;
        }

        return aggregationNode.hasNonEmptyGroupingSet() &&
                aggregationNode.getGroupingSetCount() == 1 &&
                aggregationNode.getStep() == SINGLE;
    }

    /**
     * This rule supports decorrelation of UnnestNode meeting certain conditions:
     * - the UnnestNode should be based on correlation symbols, that is: either unnest correlation symbols directly,
     * or unnest symbols produced by a projection that uses only correlation symbols.
     * - the UnnestNode should not have any replicate symbols,
     * - the UnnestNode should be of type LEFT,
     * - the UnnestNode should not have a filter.
     */
    private static boolean isSupportedUnnest(PlanNode node, List<Symbol> correlation, Lookup lookup)
    {
        if (!(node instanceof UnnestNode unnestNode)) {
            return false;
        }

        List<Symbol> unnestSymbols = unnestNode.getMappings().stream()
                .map(UnnestNode.Mapping::getInput)
                .collect(toImmutableList());
        PlanNode unnestSource = lookup.resolve(unnestNode.getSource());
        boolean basedOnCorrelation = ImmutableSet.copyOf(correlation).containsAll(unnestSymbols) ||
                unnestSource instanceof ProjectNode && ImmutableSet.copyOf(correlation).containsAll(SymbolsExtractor.extractUnique(((ProjectNode) unnestSource).getAssignments().getExpressions()));

        return isScalar(unnestNode.getSource(), lookup) &&
                unnestNode.getReplicateSymbols().isEmpty() &&
                basedOnCorrelation &&
                unnestNode.getJoinType() == LEFT &&
                (unnestNode.getFilter().isEmpty() || unnestNode.getFilter().get().equals(TRUE_LITERAL));
    }

    private static PlanNode rewriteNodeSequence(PlanNode root, List<Symbol> leftOutputs, PlanNode sequenceSource, PlanNodeId correlatedUnnestId, Lookup lookup)
    {
        // bottom of sequence reached -- attach the rewritten source node
        if (root.getId().equals(correlatedUnnestId)) {
            return sequenceSource;
        }

        PlanNode source = rewriteNodeSequence(lookup.resolve(getOnlyElement(root.getSources())), leftOutputs, sequenceSource, correlatedUnnestId, lookup);

        if (root instanceof AggregationNode aggregationNode) {
            return withGrouping(aggregationNode, leftOutputs, source);
        }

        if (root instanceof ProjectNode projectNode) {
            return new ProjectNode(
                    projectNode.getId(),
                    source,
                    Assignments.builder()
                            .putAll(projectNode.getAssignments())
                            .putIdentities(leftOutputs)
                            .build());
        }

        throw new IllegalStateException("unexpected node: " + root);
    }

    private static AggregationNode withGrouping(AggregationNode aggregationNode, List<Symbol> groupingSymbols, PlanNode source)
    {
        AggregationNode.GroupingSetDescriptor groupingSet = singleGroupingSet(Streams.concat(groupingSymbols.stream(), aggregationNode.getGroupingKeys().stream())
                .distinct()
                .collect(toImmutableList()));

        return singleAggregation(
                aggregationNode.getId(),
                source,
                aggregationNode.getAggregations(),
                groupingSet);
    }
}
